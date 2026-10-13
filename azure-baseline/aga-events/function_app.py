# Copyright 2026, American Go Association, All rights reserved

"""Azure Functions entry point for the first AGA Events vertical slice."""

from __future__ import annotations

import base64
import json
import os
from urllib.parse import urlsplit
from io import BytesIO
from html import escape
from pathlib import Path
from typing import Any

import azure.functions as func
import qrcode
import qrcode.image.svg

from event_domain import (
    EventError,
    normalize_contact,
    normalize_occurrence_ids,
    normalize_occurrence_input,
    utc_now,
)
from sql_repository import SqlEventRepository, get_sql_connection_string


app = func.FunctionApp()
_REPOSITORY: SqlEventRepository | None = None


@app.function_name(name='WorkshopAdminScript')
@app.route(route='events-assets/workshop-admin.js', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_admin_script(req):
    return func.HttpResponse((Path(__file__).parent / 'workshop_admin.js').read_text(encoding='utf-8'),
                             headers=_security_headers('text/javascript; charset=utf-8'))


def _security_headers(content_type: str) -> dict[str, str]:
    return {
        "Content-Type": content_type,
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Content-Security-Policy": (
            "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; frame-ancestors 'self' https://*.clubexpress.com "
            "https://*.usgo.org https://usgo.org"
        ),
    }


def _json_response(payload: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, ensure_ascii=False),
        status_code=status_code,
        headers=_security_headers("application/json; charset=utf-8"),
    )


def _error_response(error: Exception) -> func.HttpResponse:
    if isinstance(error, EventError):
        return _json_response(
            {
                "ok": False,
                "error": {"code": error.code, "message": str(error), "details": error.details},
            },
            error.status_code,
        )
    return _json_response(
        {"ok": False, "error": {"code": "server_error", "message": "The request could not be completed."}},
        500,
    )


def _repository() -> SqlEventRepository:
    global _REPOSITORY
    if _REPOSITORY is None:
        connection_string = get_sql_connection_string()
        if not connection_string:
            raise RuntimeError("Missing SQL_CONNECTION_STRING application setting.")
        _REPOSITORY = SqlEventRepository(connection_string)
    return _REPOSITORY


def _request_json(req: func.HttpRequest) -> dict[str, Any]:
    try:
        payload = req.get_json()
    except ValueError as exc:
        raise EventError("Request body must be valid JSON.") from exc
    if not isinstance(payload, dict):
        raise EventError("Request body must be a JSON object.")
    return payload


def _event_page_url(req: func.HttpRequest, slug: str) -> str:
    configured_base = os.environ.get("EVENTS_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if configured_base:
        return f"{configured_base}/events/{slug}"
    marker = "/events/"
    request_url = req.url
    if marker in request_url:
        prefix = request_url.split(marker, 1)[0]
        return f"{prefix}/events/{slug}"
    return request_url.rstrip("/")


def _decode_client_principal(req: func.HttpRequest) -> tuple[str | None, set[str]]:
    principal_id = req.headers.get("x-ms-client-principal-id")
    roles: set[str] = set()
    encoded = req.headers.get("x-ms-client-principal")
    if encoded:
        try:
            padding = "=" * (-len(encoded) % 4)
            principal = json.loads(base64.b64decode(encoded + padding).decode("utf-8"))
            principal_id = principal.get("userId") or principal_id
            for claim in principal.get("claims", []):
                claim_type = str(claim.get("typ", ""))
                if claim_type in {"roles", "role", "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"}:
                    roles.add(str(claim.get("val", "")))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError, TypeError, AttributeError):
            return None, set()
    if os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
        local_id = os.environ.get("EVENTS_LOCAL_ORGANIZER_PRINCIPAL_ID")
        if local_id and req.headers.get("x-events-local-principal") == local_id:
            return local_id, {"Events.Organizer"}
    return principal_id, roles


def _organizer_principal(req: func.HttpRequest) -> str:
    principal_id, roles = _decode_client_principal(req)
    if not principal_id or not roles.intersection({"Events.Organizer", "Events.PlatformAdmin"}):
        error = EventError("Organizer sign-in is required.")
        error.status_code = 401
        error.code = "authentication_required"
        raise error
    if req.method not in {'GET', 'HEAD', 'OPTIONS'}:
        # Custom header prevents cross-site HTML forms; no cross-origin admin CORS.
        origin = req.headers.get('origin')
        request_origin = urlsplit(req.url)
        canonical = os.environ.get('EVENTS_PUBLIC_BASE_URL', '').rstrip('/')
        expected = canonical or f'{request_origin.scheme}://{request_origin.netloc}'
        if req.headers.get('x-events-admin') != '1' or (origin and origin != expected):
            from event_domain import EventForbidden
            raise EventForbidden('Refresh the workshop manager and try again.')
    return principal_id


@app.function_name(name="EventsPublicPage")
@app.route(route="events/{slug}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def events_public_page(req: func.HttpRequest) -> func.HttpResponse:
    markup = (Path(__file__).resolve().parent / "event_registration.html").read_text(encoding="utf-8")
    return func.HttpResponse(
        markup,
        status_code=200,
        headers=_security_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="EventsManagePage")
@app.route(route="events/manage/{slug}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def events_manage_page(req: func.HttpRequest) -> func.HttpResponse:
    markup = (Path(__file__).resolve().parent / "event_manage.html").read_text(encoding="utf-8")
    return func.HttpResponse(
        markup,
        status_code=200,
        headers=_security_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="EventsPublicData")
@app.route(route="api/events/{slug}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def events_public_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = _repository().get_public_event(req.route_params["slug"], utc_now())
        return _json_response({"ok": True, "event": payload})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsRegisterFree")
@app.route(route="api/events/{slug}/registrations", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def events_register_free(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = _request_json(req)
        contact = normalize_contact(payload.get("contact") or {})
        occurrence_ids = normalize_occurrence_ids(payload.get("occurrence_ids"))
        idempotency_key = (req.headers.get("idempotency-key") or payload.get("idempotency_key") or "").strip()
        if not idempotency_key or len(idempotency_key) > 100:
            raise EventError("An idempotency key of 1 to 100 characters is required.")
        result = _repository().register_free(
            slug=req.route_params["slug"],
            occurrence_ids=occurrence_ids,
            contact=contact,
            idempotency_key=idempotency_key,
            now=utc_now(),
            referral_chapter_id=payload.get('referral_chapter_id'),
        )
        return _json_response({"ok": True, "registration": result}, 201)
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsQrCode")
@app.route(route="events/{slug}/qr.svg", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def events_qr_code(req: func.HttpRequest) -> func.HttpResponse:
    target = _event_page_url(req, req.route_params["slug"])
    if req.params.get('ref'):
        try:
            chapter = _repository().get_referral_chapter(req.params['ref'])
            if chapter:
                target += f"?ref={chapter['chapter_id']}"
        except Exception as exc:
            return _error_response(exc)
    image = qrcode.make(target, image_factory=qrcode.image.svg.SvgPathImage, border=4)
    buffer = BytesIO()
    image.save(buffer)
    headers = _security_headers("image/svg+xml; charset=utf-8")
    headers["Cache-Control"] = "public, max-age=86400"
    headers["Content-Disposition"] = f'inline; filename="{req.route_params["slug"]}-registration-qr.svg"'
    return func.HttpResponse(body=buffer.getvalue(), status_code=200, headers=headers)


@app.function_name(name='WorkshopQrDownload')
@app.route(route='events/{slug}/workshop-qr/{format}', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_qr_download(req):
    from workshop_qr import badge_png, badge_svg, DOWNLOAD_SIZE
    slug=req.route_params['slug']
    format=req.route_params['format']
    if slug not in {'learn-go-in-an-hour','learn-go-in-an-hour-rehearsal'} or format not in {'png','svg'}:
        return _json_response({'ok':False,'error':{'message':'Download not found.'}},404)
    target=_event_page_url(req,slug)
    chapter=None
    if req.params.get('ref'):
        try:
            chapter=_repository().get_referral_chapter(req.params['ref'])
            if not chapter:
                return _json_response({'ok':False,'error':{'message':'Chapter not found. Please select your chapter again.'}},404)
            target+=f"?ref={chapter['chapter_id']}"
        except Exception as exc:
            return _error_response(exc)
    headers=_security_headers('image/png' if format=='png' else 'image/svg+xml; charset=utf-8')
    headers['Content-Disposition']=f'attachment; filename="free-beginners-workshop-qr.{format}"'
    if chapter:
        headers['Content-Disposition']=f'attachment; filename="free-beginners-workshop-chapter-{chapter["chapter_id"]}-qr.{format}"'
    is_test=slug.endswith('-rehearsal')
    size=1200 if chapter else DOWNLOAD_SIZE
    return func.HttpResponse(body=badge_png(target,size=size,test_label=is_test) if format=='png' else badge_svg(target,size=size,test_label=is_test),headers=headers)


@app.function_name(name='WorkshopQrCard')
@app.route(route='events/{slug}/workshop-qr', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_qr_card(req):
    slug=req.route_params['slug']
    if slug not in {'learn-go-in-an-hour','learn-go-in-an-hour-rehearsal'}:
        return _json_response({'ok':False,'error':{'message':'QR card not found.'}},404)
    registration_url=_event_page_url(req,slug)
    card_url=registration_url+'/workshop-qr'
    png_url=card_url+'/png'
    chapter_label=''
    if req.params.get('ref'):
        try:
            chapter=_repository().get_referral_chapter(req.params['ref'])
            if not chapter:
                return _json_response({'ok':False,'error':{'message':'Chapter not found. Please select your chapter again.'}},404)
            query=f"?ref={chapter['chapter_id']}"
            registration_url+=query
            card_url+=query
            png_url+=query
            chapter_label=chapter['chapter_name']+' ('+chapter['chapter_code']+')'
        except Exception as exc:
            return _error_response(exc)
    markup=(Path(__file__).parent/'workshop_qr_page.html').read_text(encoding='utf-8')
    markup=markup.replace('__REGISTRATION_URL__',escape(registration_url,quote=True)).replace('__PNG_URL__',escape(png_url,quote=True)).replace('__CARD_URL__',escape(card_url,quote=True)).replace('__CHAPTER_LABEL__',escape(chapter_label))
    if slug.endswith('-rehearsal'):
        markup=markup.replace('Free Beginners Go Workshop','TEST — Free Beginners Go Workshop').replace('American Go Association</p>','American Go Association · Rehearsal</p>')
    return func.HttpResponse(markup,headers=_security_headers('text/html; charset=utf-8'))


@app.function_name(name='WorkshopChapterQrPage')
@app.route(route='events/{slug}/chapter-qr', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_chapter_qr_page(req):
    if req.route_params['slug'] not in {'learn-go-in-an-hour','learn-go-in-an-hour-rehearsal'}:
        return _json_response({'ok':False,'error':{'message':'Workshop not found.'}},404)
    return func.HttpResponse((Path(__file__).parent/'workshop_chapter_qr.html').read_text(encoding='utf-8'),
                             headers=_security_headers('text/html; charset=utf-8'))


@app.function_name(name='WorkshopReferralChapters')
@app.route(route='api/workshop-chapters', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_referral_chapters(req):
    try:
        return _json_response({'ok':True,'chapters':_repository().list_referral_chapters()})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name='WorkshopQrShareScript')
@app.route(route='events-assets/workshop-qr-share.js', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_qr_share_script(req):
    return func.HttpResponse((Path(__file__).parent/'workshop_qr_share.js').read_text(encoding='utf-8'),
                             headers=_security_headers('text/javascript; charset=utf-8'))


@app.function_name(name='WorkshopRegistrationStatistics')
@app.route(route='api/events/manage/{event_id:int}/statistics', methods=['GET'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_registration_statistics(req):
    try:
        principal_id = _organizer_principal(req)
        value = req.params.get('occurrence_id')
        occurrence_id = None
        if value is not None:
            if not value.isascii() or not value.isdigit() or len(value)>18 or int(value)<1:
                raise EventError('Choose a valid workshop date.')
            occurrence_id = int(value)
        result = _repository().registration_statistics(event_id=int(req.route_params['event_id']),
            principal_id=principal_id,occurrence_id=occurrence_id,now=utc_now())
        return _json_response({'ok':True,'statistics':result})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsManageAddOccurrence")
@app.route(
    route="api/events/manage/{event_id:int}/occurrences",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def events_manage_add_occurrence(req: func.HttpRequest) -> func.HttpResponse:
    try:
        principal_id = _organizer_principal(req)
        occurrence = normalize_occurrence_input(_request_json(req))
        result = _repository().add_occurrence(
            event_id=int(req.route_params["event_id"]),
            principal_id=principal_id,
            occurrence=occurrence,
            now=utc_now(),
        )
        return _json_response({"ok": True, "occurrence": result}, 201)
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsManageListOccurrences")
@app.route(
    route="api/events/manage/{event_id:int}/occurrences",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def events_manage_list_occurrences(req: func.HttpRequest) -> func.HttpResponse:
    try:
        principal_id = _organizer_principal(req)
        result = _repository().list_managed_occurrences(
            event_id=int(req.route_params["event_id"]),
            principal_id=principal_id,
            now=utc_now(),
        )
        return _json_response({"ok": True, **result})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsManageUpdateOccurrence")
@app.route(
    route="api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}",
    methods=["PATCH"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def events_manage_update_occurrence(req: func.HttpRequest) -> func.HttpResponse:
    try:
        principal_id = _organizer_principal(req)
        occurrence = normalize_occurrence_input(_request_json(req))
        result = _repository().update_occurrence(
            event_id=int(req.route_params["event_id"]),
            occurrence_id=int(req.route_params["occurrence_id"]),
            principal_id=principal_id,
            occurrence=occurrence,
            now=utc_now(),
        )
        return _json_response({"ok": True, "occurrence": result})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name="EventsHealth")
@app.route(route="api/events/health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def events_health(req: func.HttpRequest) -> func.HttpResponse:
    return _json_response({"ok": True, "service": "aga-events", "time": utc_now().isoformat()})


@app.function_name(name='WorkshopRoster')
@app.route(route='api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/roster', methods=['GET', 'POST'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_roster(req):
    try:
        principal = _organizer_principal(req)
        event_id = int(req.route_params['event_id'])
        occurrence_id = int(req.route_params['occurrence_id'])
        if req.method == 'GET':
            return _json_response({'ok': True, **_repository().list_roster(
                event_id=event_id, occurrence_id=occurrence_id, principal_id=principal)})
        body = _request_json(req)
        key = str(body.get('idempotency_key') or '')
        if not key or len(key) > 100:
            raise EventError('A submission key is required.')
        send_confirmation = body.get('send_confirmation_email', True)
        if not isinstance(send_confirmation, bool):
            raise EventError('Send confirmation email must be true or false.')
        # Resolve slug only through the authorized event, then recheck scope in registration transaction.
        with _repository()._workshop_transaction(event_id, principal) as (_, event):
            slug = event['slug']
        result = _repository().register_free(slug=slug, occurrence_ids=[occurrence_id],
            contact=normalize_contact(body.get('contact') or {}), idempotency_key=key,
            now=utc_now(), principal_id=principal, managed_event_id=event_id,
            send_confirmation_email=send_confirmation)
        return _json_response({'ok': True, 'registration': result}, 201)
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name='WorkshopSignup')
@app.route(route='api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/roster/{selection_id:int}', methods=['PATCH', 'DELETE'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_signup(req):
    try:
        principal = _organizer_principal(req)
        args = {k: int(req.route_params[k]) for k in ('event_id', 'occurrence_id', 'selection_id')}
        args.update(principal_id=principal, now=utc_now())
        if req.method == 'DELETE':
            result = _repository().cancel_signup(**args)
        else:
            payload = _request_json(req)
            if set(payload)=={'ogs_handle'}:
                result = _repository().set_ogs_handle(**args, ogs_handle=payload['ogs_handle'])
            elif set(payload)=={'attendance'}:
                result = _repository().set_attendance(**args, attendance=payload['attendance'])
            else:
                raise EventError('Update an OGS handle or attendance, one at a time.')
        return _json_response({'ok': True, **result})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name='WorkshopCancelDate')
@app.route(route='api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}', methods=['DELETE'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_cancel_date(req):
    try:
        principal = _organizer_principal(req)
        result = _repository().cancel_occurrence(event_id=int(req.route_params['event_id']),
            occurrence_id=int(req.route_params['occurrence_id']), principal_id=principal, now=utc_now())
        return _json_response({'ok': True, **result})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name='WorkshopSettings')
@app.route(route='api/events/manage/{event_id:int}/settings', methods=['GET', 'PUT'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_settings(req):
    try:
        principal = _organizer_principal(req)
        args = dict(event_id=int(req.route_params['event_id']), principal_id=principal)
        if req.method == 'GET':
            result = _repository().get_workshop_settings(**args)
        else:
            result = _repository().save_workshop_settings(**args, payload=_request_json(req), now=utc_now())
        from workshop_providers import email_delivery_config
        delivery=email_delivery_config()
        delivery['enabled']=os.environ.get('EVENTS_EMAIL_ENABLED')=='true'
        return _json_response({'ok': True, 'settings': result, 'delivery': delivery})
    except Exception as exc:
        return _error_response(exc)


@app.function_name(name='WorkshopNotificationsTimer')
@app.timer_trigger(schedule='0 * * * * *', arg_name='timer', run_on_startup=False, use_monitor=True)
def workshop_notifications_timer(timer):
    if os.environ.get('EVENTS_EMAIL_ENABLED')=='true' or os.environ.get('EVENTS_ZOOM_ENABLED')=='true':
        from workshop_worker import run_worker
        run_worker(_repository())


@app.function_name(name='WorkshopRehearsalClock')
@app.route(route='api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/rehearsal-clock', methods=['GET','POST'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_rehearsal_clock(req):
    try:
        principal=_organizer_principal(req)
        args=dict(event_id=int(req.route_params['event_id']),occurrence_id=int(req.route_params['occurrence_id']),principal_id=principal,now=utc_now())
        if req.method=='POST':
            body=_request_json(req)
            if set(body)!={'expected_stage'} or type(body['expected_stage']) is not int:raise EventError('Provide the current rehearsal step.')
            args['expected_stage']=body['expected_stage']
        return _json_response({'ok':True,'clock':_repository().rehearsal_clock(**args)})
    except Exception as exc:return _error_response(exc)


@app.function_name(name='WorkshopMeeting')
@app.route(route='api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/meeting', methods=['GET','PUT','PATCH'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_meeting(req):
    try:
        principal=_organizer_principal(req)
        args=dict(event_id=int(req.route_params['event_id']),occurrence_id=int(req.route_params['occurrence_id']),principal_id=principal)
        if req.method=='GET':result=_repository().get_meeting(**args)
        elif req.method=='PATCH':
            from workshop_providers import ZoomClient
            result=_repository().reconcile_meeting(**args,payload=_request_json(req),now=utc_now(),zoom=ZoomClient())
        else:result=_repository().configure_meeting(**args,payload=_request_json(req),now=utc_now())
        return _json_response({'ok':True,'meeting':result})
    except Exception as exc:return _error_response(exc)


@app.function_name(name='WorkshopEmailReconcile')
@app.route(route='api/events/manage/{event_id:int}/emails/{email_id:int}', methods=['PATCH'], auth_level=func.AuthLevel.ANONYMOUS)
def workshop_email_reconcile(req):
    try:
        principal=_organizer_principal(req);body=_request_json(req)
        result=_repository().reconcile_email(event_id=int(req.route_params['event_id']),email_id=int(req.route_params['email_id']),
            principal_id=principal,resolution=body.get('resolution'),note=body.get('note'),now=utc_now())
        return _json_response({'ok':True,**result})
    except Exception as exc:return _error_response(exc)
