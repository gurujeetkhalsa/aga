"""Workshop messages with plain-text fallbacks and a portable calendar attachment."""
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import formataddr
from html import escape
import re
from zoneinfo import ZoneInfo
from workshop_domain import validate_join_url


OGS_REGISTER_URL = 'https://online-go.com/register'


def attended_followup(data, settings):
    body=settings['followup_text'].strip()
    def inline(text):
        return re.sub(r'\*\*([^*\n]+)\*\*',r'<strong>\1</strong>',escape(text))
    blocks=[]
    for block in re.split(r'\n\s*\n',body):
        lines=block.splitlines()
        if lines and all(line.startswith('- ') for line in lines):
            blocks.append('<ul>'+''.join(f'<li>{inline(line[2:])}</li>' for line in lines)+'</ul>')
        else:
            blocks.append('<p>'+inline(block).replace('\n','<br>')+'</p>')
    plain_body=re.sub(r'\*\*([^*\n]+)\*\*',r'\1',body)
    plain=f"Hi {data['first_name']},\n\n{plain_body}\n\nBest,\n{settings['instructor_name']}"
    html=('<!doctype html><html><body>'+f"<p>Hi {escape(data['first_name'])},</p>"+
          ''.join(blocks)+f"<p>Best,<br>{escape(settings['instructor_name'])}</p></body></html>")
    return plain,html


def no_show_followup(data, settings):
    """Milan's invitation with an email-compatible QR card and web actions."""
    registration_url = data['registration_url']
    card_url = registration_url.rstrip('/') + '/workshop-qr'
    download_url = card_url + '/png'
    paragraphs = [
        f"Hi {data['first_name']},",
        "We noticed that you registered for the Beginner Go Workshop but weren't able to attend.",
        "No problem! If you'd still like to learn Go, you are welcome to attend a future Beginner Go Workshop.",
        "The workshop is also the first step toward our 8-week Beginner Go Course, which continues where the workshop ends and gives you more time to learn, practice, and play Go.",
        "Scan the QR code or tap the card below to see upcoming Beginner Go Workshops and choose a session that works for you.",
        "Once you attend a workshop, you will receive more information about the 8-week course when registration opens.",
        "We hope to see you at a future workshop!",
        f"Best,\n{settings['instructor_name']}",
    ]
    plain = list(paragraphs)
    plain[4] = (f'See upcoming Beginner Go Workshops and choose a session that works for you:\n{registration_url}'
                f'\n\nOpen or share the QR card:\n{card_url}\n\nDownload the QR image:\n{download_url}')
    html = [escape(p).replace('\n', '<br>') for p in paragraphs]
    html[1] = html[1].replace('Beginner Go Workshop', '<strong>Beginner Go Workshop</strong>')
    html[3] = html[3].replace('8-week Beginner Go Course', '<strong>8-week Beginner Go Course</strong>')
    blocks=[f'<p>{p}</p>' for p in html]
    card=escape(card_url,quote=True);download=escape(download_url,quote=True);registration=escape(registration_url,quote=True)
    blocks[4] = (f'<p><strong>{html[4]}</strong></p>'
        '<table role="presentation" width="360" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;max-width:360px;border:1px solid #d9d3c8;border-radius:14px;background:#ffffff;font-family:Arial,sans-serif">'
        f'<tr><td colspan="2"><a href="{card}" aria-label="Open the interactive QR card">'
        '<img src="cid:upcoming-workshops@usgo.org" width="360" height="360" '
        'style="display:block;width:100%;max-width:360px;height:auto;border:0;border-radius:14px 14px 0 0" '
        'alt="Scan or tap to see upcoming Beginner Go Workshops"></a></td></tr>'
        '<tr><td align="left" style="padding:4px 8px 8px">'
        f'<a href="{download}" style="display:inline-block;padding:12px;color:#a71920;text-decoration:none;font-size:15px">↓ Download</a></td>'
        '<td align="right" style="padding:4px 8px 8px">'
        f'<a href="{card}" style="display:inline-block;padding:12px;color:#a71920;text-decoration:none;font-size:15px">↗ Share card</a></td></tr></table>'
        '<table role="presentation" width="360" cellpadding="0" cellspacing="0" border="0" '
        'style="width:100%;max-width:360px;margin-top:14px;font-family:Arial,sans-serif">'
        '<tr><td align="center" bgcolor="#a71920" style="border-radius:10px">'
        f'<a href="{registration}" style="display:block;padding:13px 16px;color:#ffffff;text-decoration:none;font-weight:bold">'
        'View upcoming workshops and register</a></td></tr></table>')
    return '\n\n'.join(plain), '<!doctype html><html><body>' + ''.join(blocks) + '</body></html>'


def preparation_reminder(data, settings, date):
    """Milan's welcome/preparation email for confirmed places and early reminders."""
    zoom = validate_join_url(data['join_url'])
    paragraphs = [
        f"Hi {data['first_name']},",
        (f'Thank you so much for signing up for our beginner Go workshop! Your place is confirmed for {date}.'
         if data['kind']=='confirmed' else
         f'Thank you so much for your interest in our beginner Go workshop, scheduled for {date}.'),
        'The workshop will be interactive, so you’ll need to create an account on the Online Go Server (OGS) '
        'in order to participate. It’s quick and easy to set up, and you don’t even need to provide your email '
        'address. Once you’ve created your OGS handle, please email it to me.',
        'We’ll meet on Zoom and use OGS for playing and demonstrations. During the first 30 minutes, I’ll guide '
        'you through how to navigate the server, explain the basic rules, and help you prepare for your first '
        'online game. In the second half of the workshop, you’ll play a game with my guidance.',
        'Shortly after the workshop, an online 8-week course organized by the American Go Association (AGA) will be available for those '
        'who would like to continue learning Go.',
        'Zoom link for the workshop',
        f'I hope to see you on {date} and help you discover this wonderful game.',
        f'If you need to cancel or have any questions, please email me at {settings["sender_email"]}.',
        'Best,',
        settings['instructor_name'],
    ]
    plain = list(paragraphs)
    plain[2] = plain[2].replace('Online Go Server (OGS)', f'Online Go Server (OGS): {OGS_REGISTER_URL}')
    plain[5] += f': {zoom}'
    html = [escape(p) for p in paragraphs]
    html[2] = html[2].replace('Online Go Server (OGS)', f'<a href="{OGS_REGISTER_URL}">Online Go Server (OGS)</a>')
    html[5] = f'<a href="{escape(zoom, quote=True)}">Zoom link for the workshop</a>'
    return '\n\n'.join(plain), '<!doctype html><html><body>' + ''.join(f'<p>{p}</p>' for p in html) + '</body></html>'


def next_day_reminder(data, settings, date):
    """A focused next-day reminder with the participation requirement up front."""
    zoom = validate_join_url(data['join_url'])
    requirement = 'You need an OGS account and handle (username) to participate in the workshop.'
    paragraphs = [
        f"Hi {data['first_name']},",
        f'Your beginner Go workshop starts tomorrow, {date}.',
        requirement + ' We’ll use the Online Go Server (OGS) for interactive activities and to play your game.',
        'If you don’t have an account yet, please create one today on the Online Go Server (OGS). '
        'If you already have an OGS account, you can use your existing handle.',
        'If you haven’t already sent me your OGS handle, please reply to this email with it today '
        'so I can find you on OGS and help you get started.',
        'Please sign in to OGS before the workshop and join Zoom a few minutes early. '
        'The host will admit you from the waiting room.',
        'Zoom link for the workshop',
        'I look forward to playing Go with you tomorrow!',
        f'If you need help setting up OGS, need to cancel, or have any questions, please email me at {settings["sender_email"]}.',
        f'Best,\n{settings["instructor_name"]}',
    ]
    plain = list(paragraphs)
    plain[3] += f'\n{OGS_REGISTER_URL}'
    plain[6] += f': {zoom}'
    html = [escape(p).replace('\n', '<br>') for p in paragraphs]
    html[1] = f'<strong>{html[1]}</strong>'
    html[2] = html[2].replace(requirement, f'<strong>{requirement}</strong>')
    html[3] = html[3].replace('Online Go Server (OGS)', f'<a href="{OGS_REGISTER_URL}">Online Go Server (OGS)</a>')
    html[4] = f'<strong>{html[4]}</strong>'
    html[6] = f'<a href="{escape(zoom, quote=True)}">Zoom link for the workshop</a>'
    return '\n\n'.join(plain), '<!doctype html><html><body>' + ''.join(f'<p>{p}</p>' for p in html) + '</body></html>'


def _ical_text(value):
    return str(value).replace('\\', '\\\\').replace('\n', '\\n').replace(';', '\\;').replace(',', '\\,').replace('\r', '')


def calendar_attachment(data, now):
    def stamp(dt):
        from datetime import timezone
        return dt.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    # Fold on UTF-8 byte boundaries so long titles/URLs remain standards compliant.
    fields=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//AGA//Workshops//EN','METHOD:PUBLISH',
        'BEGIN:VEVENT',f"UID:aga-workshop-{data['occurrence_id']}@usgo.org",f'DTSTAMP:{stamp(now)}',
        f"DTSTART:{stamp(data['starts_at_utc'])}",f"DTEND:{stamp(data['ends_at_utc'])}",
        f"SUMMARY:{_ical_text(data['title'])}",f"LOCATION:{_ical_text(data['join_url'])}",'END:VEVENT','END:VCALENDAR']
    folded=[]
    for line in fields:
        part=''
        for char in line:
            if len((part+char).encode('utf-8'))>74:
                folded.append(part);part=' '
            part+=char
        folded.append(part)
    return ('\r\n'.join(folded)+'\r\n').encode('utf-8')


def build_message(data, settings, now):
    kind=data['kind']
    local=data['starts_at_utc'].astimezone(ZoneInfo(data['time_zone']))
    date=f"{local.strftime('%A, %B')} {local.day}, {local.year} at {local.strftime('%I:%M').lstrip('0')} {local.strftime('%p %Z')}"
    title=data['title']
    introductions={
        'confirmed': ('Registration confirmed', 'Your place is confirmed.'),
        'waitlisted': ('You are on the waitlist', 'You are on the waitlist. We will email you if a place becomes available. Please wait for a confirmation before joining.'),
        'cancelled': ('Registration cancelled', 'Your registration has been cancelled.'),
        'workshop_cancelled': ('Workshop cancelled', 'This workshop has been cancelled. Your registration has also been cancelled.'),
    }
    subject,intro=introductions.get(kind, ('Workshop reminder', 'Your Go workshop is coming up.'))
    if kind=='reminder_1':
        intro='Your Go workshop is coming up very shortly.'
    if kind=='reminder_24':
        subject='Workshop tomorrow — OGS handle required'
    greeting='Hi' if kind in {'reminder_1','waitlisted','cancelled','workshop_cancelled'} else 'Hello'
    lines=[f"{greeting} {data['first_name']},",'',intro,'',title]
    if kind not in {'reminder_1','waitlisted','cancelled','workshop_cancelled'}:
        lines.append(date)
    lines.append(f"Time zone: {data['time_zone']}")
    attach=kind=='confirmed' or kind.startswith('reminder_')
    if attach:
        link=validate_join_url(data['join_url'])
        instructor=settings['instructor_name']
        if kind=='reminder_1' and instructor=='Milan':
            instructor='Milan Mladenovic'
        lines.extend([f"Instructor: {instructor}",'',f'Join on Zoom: {link}',
                      'Please join a few minutes early. The host will admit you from the waiting room.'])
    signature=settings['instructor_name'].split()[0] if kind in {'reminder_1','waitlisted','cancelled','workshop_cancelled'} else 'American Go Association'
    lines.extend(['',f"To cancel or ask a question, reply to {settings['sender_email']}.",'',signature])
    if kind=='followup':
        if data.get('attendance')=='attended':
            subject='Continue learning Go'
            body=settings['followup_text'].strip()
        elif data.get('attendance')=='absent':
            subject='Join another beginner Go workshop'
            body=''
        else:
            raise ValueError('Record attendance before preparing a follow-up email.')
        lines=[f"Hi {data['first_name']},",'',body,'','Best,',settings['instructor_name']]
    message=EmailMessage(policy=SMTP)
    message['From']=settings['sender_email'];message['Reply-To']=settings['sender_email']
    name=' '.join(str(data.get(k,'')).strip() for k in ('first_name','last_name')).strip()
    message['To']=formataddr((name,data['email']));message['Subject']=f'{subject}: {title}'
    message['Message-ID']=f"<aga-workshop-email-{data['email_id']}@usgo.org>"
    lead_hours=kind.removeprefix('reminder_') if kind.startswith('reminder_') else ''
    if kind=='followup' and data.get('attendance')=='absent':
        from workshop_qr import badge_png
        plain,html=no_show_followup(data,settings)
        message.set_content(plain)
        message.add_alternative(html,subtype='html')
        message.get_body(preferencelist=('html',)).add_related(
            badge_png(data['registration_url'],test_label=bool(settings.get('is_test'))),
            maintype='image',subtype='png',cid='<upcoming-workshops@usgo.org>',
            disposition='inline')
    elif kind=='followup' and data.get('attendance')=='attended' and (
            '**' in settings['followup_text'] or re.search(r'^- ',settings['followup_text'],re.M)):
        plain,html=attended_followup(data,settings)
        message.set_content(plain)
        message.add_alternative(html,subtype='html')
    elif kind=='reminder_24':
        plain,html=next_day_reminder(data,settings,date)
        message.set_content(plain)
        message.add_alternative(html,subtype='html')
    elif kind=='confirmed' or (lead_hours.isdigit() and int(lead_hours)>=24):
        plain,html=preparation_reminder(data,settings,date)
        message.set_content(plain)
        message.add_alternative(html,subtype='html')
    elif kind=='reminder_1':
        message.set_content('\n'.join(lines))
        paragraphs=[f'<a href="{escape(link, quote=True)}">Join the workshop on Zoom</a>'
                    if line==f'Join on Zoom: {link}' else escape(line) for line in lines if line]
        message.add_alternative('<!doctype html><html><body>'+''.join(f'<p>{p}</p>' for p in paragraphs)+'</body></html>',subtype='html')
    else:
        message.set_content('\n'.join(lines))
    if attach:
        message.add_attachment(calendar_attachment(data,now),maintype='text',subtype='calendar',filename='workshop.ics')
    return message
