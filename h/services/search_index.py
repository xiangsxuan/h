from h import storage
from h.events import AnnotationTransformEvent
from h.models import Annotation
from h.presenters import AnnotationSearchIndexPresenter
from h.search.index import BatchIndexer


class SearchIndexService:
    def __init__(self, es_client, db, request, settings_service):
        self._request = request
        self._es_client = es_client
        self._db = db
        self._settings = settings_service

    def add_annotation_by_id(self, id_):
        annotation = storage.fetch_annotation(self._db, id_)
        if not annotation:
            # TODO! - Come up with a better error
            raise ValueError(f"Cannot find requested annotation: {id_}")

        return self.add_annotation(annotation)

    def add_annotation(self, annotation):
        if not isinstance(annotation, Annotation):
            raise ValueError(f"Expected 'Annotation' type, not {annotation}")

        self._add_annotation(annotation)

        # If a reindex is running at the moment, add annotation to the new
        # index as well.
        future_index = self.current_reindex_name
        if future_index is not None:
            self._add_annotation(annotation, target_index=future_index)

        if annotation.is_reply:
            self.add_annotation(annotation.thread_root_id)

    def add_annotations_by_id(self, ids):
        indexer = BatchIndexer(self._db, self._es_client, self._request)
        return indexer.index(ids)

    def delete_annotation_by_id(self, id_):
        self._delete_annotation(id_)

        # If a reindex is running at the moment, delete annotation from the
        # new index as well.
        future_index = self.current_reindex_name
        if future_index is not None:
            self._delete_annotation(id_, target_index=future_index)

    @property
    def current_reindex_name(self):
        return self._settings.get("reindex.new_index")

    def _add_annotation(self, annotation, target_index=None):
        """
        Index an annotation into the search index.

        A new annotation document will be created in the search index or,
        if the index already contains an annotation document with the same ID as
        the given annotation then it will be updated.

        :param es: the Elasticsearch client object to use
        :param annotation: the annotation to index
        :param target_index: the index name, uses default index if not given
        """
        presenter = AnnotationSearchIndexPresenter(annotation, self._request)
        annotation_dict = presenter.asdict()

        event = AnnotationTransformEvent(self._request, annotation, annotation_dict)
        self._request.registry.notify(event)

        if target_index is None:
            target_index = self._es_client.index

        self._es_client.conn.index(
            index=target_index,
            doc_type=self._es_client.mapping_type,
            body=annotation_dict,
            id=annotation_dict["id"],
        )

    def _delete_annotation(self, annotation_id, target_index=None, refresh=False):
        """
        Mark an annotation as deleted in the search index.

        This will write a new body that only contains the ``deleted`` boolean field
        with the value ``true``. It allows us to rely on Elasticsearch to complain
        about dubious operations while re-indexing when we use `op_type=create`.

        :param es: the Elasticsearch client object to use
        :param annotation_id: the annotation id whose corresponding document to
            delete from the search index
        :param target_index: the index name, uses default index if not given
        :param refresh: Force this deletion to be immediately visible to search operations
        """

        if target_index is None:
            target_index = self._es_client.index

        self._es_client.conn.index(
            index=target_index,
            doc_type=self._es_client.mapping_type,
            body={"deleted": True},
            id=annotation_id,
            refresh=refresh,
        )


def search_index_service_factory(context, request):
    return SearchIndexService(
        es_client=request.es,
        db=request.db,
        request=request,
        settings_service=request.find_service(name="settings"))
