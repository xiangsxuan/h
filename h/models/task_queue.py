from sqlalchemy import Column, DateTime, UnicodeText, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from h.db import Base


class Task(Base):
    """
    A task in our simple transactional task queue.

    See h.services.TaskQueue for more details.
    """

    __tablename__ = "task_queue"

    id = Column(UUID, server_default=func.uuid_generate_v1mc(), primary_key=True)
    enqueued_at = Column(DateTime, nullable=False, server_default=func.now())
    scheduled_at = Column(DateTime, nullable=False)
    tag = Column(UnicodeText, nullable=False)
    kwargs = Column(
        JSONB,
        server_default=text("'{}'::jsonb"),
        nullable=False,
    )

    def __repr__(self):
        attrs = ", ".join(
            f"{attr}={getattr(self, attr)}"
            for attr in (
                "id",
                "enqueued_at",
                "scheduled_at",
                "kwargs",
            )
        )
        return f"Task({attrs})"
