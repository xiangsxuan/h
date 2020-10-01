"""Celery tasks for consuming messages from our custom PostgreSQL task queue."""
import datetime
import logging

from dateutil.parser import isoparse

from h.models import Annotation, Task
from h.search.index import BatchIndexer

logger = logging.getLogger(__name__)


class TaskQueue:
    """
    A simple transactional task queue that consumes tasks from a DB table.

    The advantage that this home-grown task queue has over Celery is that the
    task queue is stored in Postgres so tasks can be added to the queue as part
    of a Postgres transaction.

    For example a new annotation can be added to the annotations table
    and a task to synchronize that annotation into Elasticsearch can be added
    to the task queue as part of the same Postgres transaction. This way it's
    not possible to add an annotation to Postgres and fail to add it to the
    task queue or vice-versa. The two can't get out of sync because they're
    part of a single Postgres transaction.

    On the other hand this task queue has far fewer features than Celery.
    Celery should be the default task queue for almost all tasks, and only
    tasks that really need Postgres transactionality should use this task
    queue.

    At the time of writing this task queue only supports a single type of task:
    synchronizing annotations from Postgres to Elasticsearch.
    """

    def __init__(self, db, es, batch_indexer, limit):
        self._db = db
        self._es = es
        self._batch_indexer = batch_indexer
        self._limit = limit

    def add(self, annotation_id, tag, scheduled_at=None):
        """Add an annotation to the queue to be synced to Elasticsearch."""
        self.add_all([annotation_id], tag, scheduled_at)

    def add_all(self, annotation_ids, tag, scheduled_at=None):
        """Add a list of annotations to the queue to be synced to Elasticsearch."""
        scheduled_at = scheduled_at or datetime.datetime.utcnow()

        self._db.add_all(
            Task(
                tag=tag,
                scheduled_at=scheduled_at,
                kwargs={"annotation_id": annotation_id},
            )
            for annotation_id in annotation_ids
        )

    def eat(self):
        """
        Synchronize annotations from Postgres to Elasticsearch.

        This method is meant to be run periodically. It's called by a Celery
        task that's scheduled periodically by
        https://github.com/hypothesis/h-periodic.

        """

        messages = self._get_messages_from_queue()

        if not messages:
            return

        annotation_ids = [message.kwargs["annotation_id"] for message in messages]
        annotations_from_db = self._get_annotations_from_db(annotation_ids)
        annotations_from_es = self._get_annotations_from_es(annotation_ids)

        deleted = []
        to_index = []
        succeeded = []

        for message in messages:
            annotation_id = message.kwargs["annotation_id"]
            annotation_from_db = annotations_from_db.get(annotation_id)
            annotation_from_es = annotations_from_es.get(annotation_id)

            if not annotation_from_db:
                deleted.append(message)
            elif annotation_from_db.deleted:
                deleted.append(message)
            elif not annotation_from_es:
                to_index.append(message)
            elif annotation_from_es["updated"] != annotation_from_db.updated:
                to_index.append(message)
            else:
                succeeded.append(message)

        if deleted:
            logger.info(
                f"Deleting {len(deleted)} messages from queue because their annotations have been deleted from the DB"
            )
        if succeeded:
            logger.info(
                f"Deleting {len(succeeded)} successfully completed messages from queue"
            )

        for message in deleted + succeeded:
            self._db.delete(message)

        if to_index:
            logger.info(f"Indexing {len(to_index)} annotations")
            self._batch_indexer.index(
                [message.kwargs["annotation_id"] for message in to_index]
            )

    def _get_messages_from_queue(self):
        return (
            self._db.query(Task)
            .filter(
                Task.scheduled_at < datetime.datetime.utcnow(),
            )
            .order_by(Task.enqueued_at)
            .limit(self._limit)
            .with_for_update(skip_locked=True)
            .all()
        )

    def _get_annotations_from_db(self, annotation_ids):
        return {
            annotation.id: annotation
            for annotation in self._db.query(
                Annotation.id, Annotation.updated, Annotation.deleted
            ).filter(Annotation.id.in_(annotation_ids))
        }

    def _get_annotations_from_es(self, annotation_ids):
        hits = self._es.conn.search(
            body={
                "_source": ["updated"],
                "query": {"ids": {"values": annotation_ids}},
                "size": len(annotation_ids),
            }
        )["hits"]["hits"]

        for hit in hits:
            updated = hit["_source"].get("updated")
            updated = isoparse(updated).replace(tzinfo=None) if updated else None
            hit["_source"]["updated"] = updated

        return {hit["_id"]: hit["_source"] for hit in hits}


def factory(context, request):
    return TaskQueue(
        db=request.db,
        es=request.es,
        batch_indexer=BatchIndexer(request.db, request.es, request),
        limit=request.registry.settings["h.es_sync_task_limit"],
    )
