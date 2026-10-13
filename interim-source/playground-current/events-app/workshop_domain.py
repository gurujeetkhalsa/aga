"""Workshop policies shared by the SQL workflows, message worker and tests."""
from datetime import timedelta
import json
from urllib.parse import urlparse, parse_qs

from event_domain import EventError, EMAIL_RE, require_text

DEFAULT_ATTENDED_FOLLOWUP = """Thank you for attending the **Beginners Go Workshop**!

We hope you enjoyed learning the basics of Go, getting familiar with OGS, and playing your first game.

If you'd like to continue learning, we will be offering an **8-week Beginners Go Course** as a continuation of the workshop.

The course will include:

- 8 weekly online classes
- Learning the rules and basic Go strategy
- Practice games and guided play
- Working toward playing complete games on the **19×19 board**

**You don't need to do anything right now.** You will receive an email when registration for the 8-week course opens, with all the details about the schedule and how to sign up.

We hope to see you again and look forward to continuing your Go journey!"""


def registration_status(statuses):
    active = set(statuses) - {'cancelled'}
    if not active:
        return 'cancelled'
    if len(active) == 1:
        return next(iter(active))
    return 'mixed'


def normalize_settings(payload):
    sender = require_text(payload.get('sender_email'), 'Sender email', 254).lower()
    host = (payload.get('zoom_host_email') or '').strip().lower()
    if not EMAIL_RE.fullmatch(sender) or (host and not EMAIL_RE.fullmatch(host)):
        raise EventError('Enter valid sender and Zoom host email addresses.')
    hours = payload.get('reminder_hours', [24, 1])
    if (not isinstance(hours, list) or len(hours) > 8 or
            any(type(h) is not int or h < 1 or h > 720 for h in hours)):
        raise EventError('Reminders must be a list of up to eight whole hours between 1 and 720.')
    followup = payload.get('followup_hours', 24)
    if type(followup) is not int or not 0 <= followup <= 720:
        raise EventError('Follow-up delay must be 0 to 720 whole hours.')
    if type(payload.get('email_enabled', False)) is not bool:
        raise EventError('Email enabled must be true or false.')
    followup_text = payload.get('followup_text')
    if not isinstance(followup_text, str) or not followup_text.strip():
        raise EventError('Follow-up text is required.')
    followup_text = followup_text.strip()
    if len(followup_text) > 4000:
        raise EventError('Follow-up text must be 4000 characters or fewer.')
    return dict(sender_email=sender, zoom_host_email=host or None,
                instructor_name=require_text(payload.get('instructor_name', 'Milan'), 'Instructor', 200),
                reminder_hours=json.dumps(sorted(set(hours), reverse=True)),
                followup_hours=followup,
                followup_text=followup_text,
                email_enabled=payload.get('email_enabled', False))


def validate_join_url(value):
    value = require_text(value, 'Zoom join URL', 2000)
    parsed = urlparse(value)
    host = parsed.hostname or ''
    if (parsed.scheme != 'https' or parsed.username or parsed.password or
            not (host == 'zoom.us' or host.endswith('.zoom.us')) or
            not parsed.path.startswith('/j/') or parsed.fragment or 'zak' in parse_qs(parsed.query)):
        raise EventError('Use the participant HTTPS Zoom /j/ joining link, not a host start link.')
    return value


def scheduled_messages(starts_at, ends_at, settings, now):
    """Skip already-past reminders when someone registers at the last minute."""
    for hours in json.loads(settings['reminder_hours']):
        due = starts_at - timedelta(hours=hours)
        if due > now:
            yield f'reminder_{hours}', due
    due = ends_at + timedelta(hours=settings['followup_hours'])
    if due > now:
        yield 'followup', due


def email_is_eligible(kind, selection_status, lifecycle, starts_at, ends_at, now):
    if kind in {'cancelled', 'workshop_cancelled'}:
        return selection_status == 'cancelled'
    if lifecycle != 'published':
        return False
    if kind == 'waitlisted':
        return selection_status == 'waitlisted' and now < starts_at
    if selection_status != 'confirmed':
        return False
    if kind == 'followup':
        return now >= ends_at
    return now < starts_at
