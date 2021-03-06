from h_pyramid_sentry import report_exception
from pyramid.events import BeforeRender, subscriber

from h import __version__, emails, storage
from h.events import AnnotationEvent
from h.exceptions import RealtimeMessageQueueError
from h.notification import reply
from h.tasks import mailer


@subscriber(BeforeRender)
def add_renderer_globals(event):
    request = event["request"]

    event["base_url"] = request.route_url("index")
    event["feature"] = request.feature

    # Add Google Analytics
    event["ga_tracking_id"] = request.registry.settings.get("ga_tracking_id")

    # Add a frontend settings object which will be rendered as JSON into the
    # page.
    event["frontend_settings"] = {}

    if "h.sentry_dsn_frontend" in request.registry.settings:
        event["frontend_settings"]["raven"] = {
            "dsn": request.registry.settings["h.sentry_dsn_frontend"],
            "release": __version__,
            "userid": request.authenticated_userid,
        }


@subscriber(AnnotationEvent)
def send_reply_notifications(
    event,
    get_notification=reply.get_notification,
    generate_mail=emails.reply_notification.generate,
    send=mailer.send.delay,
):
    """Queue any reply notification emails triggered by an annotation event."""
    request = event.request
    with request.tm:
        annotation = storage.fetch_annotation(event.request.db, event.annotation_id)
        notification = get_notification(request, annotation, event.action)
        if notification is None:
            return

        send_params = generate_mail(request, notification)
        send(*send_params)


@subscriber(AnnotationEvent)
def sync_annotation(event):
    """Ensure an annotation is synchronised to Elasticsearch."""

    # Checking feature flags opens a connection to the database. As this event
    # is processed after the main transaction has closed, we must open a new
    # transaction to ensure we don't leave an un-closed transaction
    with event.request.tm:
        search_index = event.request.find_service(name="search_index")
        search_index.handle_annotation_event(
            event, synchronous=event.request.feature("synchronous_indexing")
        )


@subscriber(AnnotationEvent)
def publish_annotation_event(event):
    """Publish an annotation event to the message queue."""
    data = {
        "action": event.action,
        "annotation_id": event.annotation_id,
        "src_client_id": event.request.headers.get("X-Client-Id"),
    }
    try:
        event.request.realtime.publish_annotation(data)

    except RealtimeMessageQueueError as err:
        report_exception(err)
