// Workshop management extension; all mutations use organizer authorization.
let workshopIsRehearsal=false;
function addWorkshopLink(container,item,isNew){
  const label=field('Zoom Link','zoom_join_url',{readonly:true,wide:true});
  const input=label.querySelector('input');input.placeholder=isNew?'Available after the workshop is added.':'Loading Zoom link…';
  container.append(label);
  if(isNew)return;
  const status=node('p',undefined,'muted');status.setAttribute('role','status');container.append(status);
  let attempts=0;
  async function refresh(){
    const {meeting}=await jsonRequest(`${adminBase(item.occurrence_id)}/meeting`);
    input.value=meeting.join_url||'';
    input.placeholder='Waiting for the workshop’s Zoom meeting.';
    status.textContent=meeting.join_url?'Participant joining link.':`Zoom status: ${meeting.sync_status}. ${meeting.sync_error||'The link fills in automatically when the meeting is ready.'}`;
    if(!meeting.join_url&&['not_configured','pending','working'].includes(meeting.sync_status)&&attempts++<12){
      setTimeout(()=>{if(container.isConnected&&!container.closest('[hidden]'))refresh().catch(e=>status.textContent=e.message)},5000);
    }
  }
  container.append(action('Refresh Zoom Link',refresh));
  refresh().catch(e=>{input.placeholder='Link unavailable';status.textContent=e.message});
}
function adminBase(oid){return `/api/events/manage/${eventData.event_id}/occurrences/${oid}`}
function node(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n}
async function adminWrite(url,method,body){const result=await jsonRequest(url,{method,headers:{'Content-Type':'application/json','X-Events-Admin':'1'},body:body===undefined?undefined:JSON.stringify(body)});if(url.includes('/roster'))await loadWorkshopStatistics();return result}
function action(label,handler){const b=node('button',label);b.type='button';b.addEventListener('click',async()=>{b.disabled=true;clearError();try{await handler()}catch(e){showError(e.message)}finally{b.disabled=false}});return b}
function actionGroup(...buttons){const group=node('div',undefined,'actions inline-actions');group.append(...buttons);return group}

let statisticsRequest=0;
function setStatisticsDates(items){
  const select=document.getElementById('statistics-date'),selected=select.value;
  const all=node('option','All workshop dates');all.value='';
  select.replaceChildren(all,...items.map(item=>{
    const option=node('option',`${workshopDate(item.starts_at_utc)}${item.lifecycle==='cancelled'?' · Cancelled':''}`);
    option.value=String(item.occurrence_id);return option;
  }));
  select.value=items.some(item=>String(item.occurrence_id)===selected)?selected:'';
}
async function loadWorkshopStatistics(){
  const version=++statisticsRequest,select=document.getElementById('statistics-date');
  const status=document.getElementById('statistics-status'),results=document.getElementById('statistics-results'),button=document.getElementById('statistics-refresh');
  status.textContent='Loading statistics…';results.hidden=true;button.disabled=true;
  try{
    const filter=select.value?'?occurrence_id='+encodeURIComponent(select.value):'';
    const data=await jsonRequest(`/api/events/manage/${eventData.event_id}/statistics${filter}`);
    if(version!==statisticsRequest)return;
    const statistics=data.statistics;
    if(!statistics?.totals||!Array.isArray(statistics.chapters))throw new Error('Statistics are unavailable. Please try again.');
    results.replaceChildren();
    const cards=node('div',undefined,'statistics-cards');
    for(const [key,label] of [['signups','Total signups'],['attended','Attended'],['absent','No-shows'],['cancelled','Cancelled']]){
      const card=node('div',undefined,'statistics-card');card.append(node('strong',String(statistics.totals[key])),node('span',label));cards.append(card);
    }
    results.append(cards);
    const columns=[['signups','Signups'],['confirmed','Confirmed'],['waitlisted','Waitlisted'],['cancelled','Cancelled'],['attended','Attended'],['absent','No-shows'],['needs_attendance','Needs attendance']];
    const wrap=node('div',undefined,'statistics-table-wrap');wrap.tabIndex=0;wrap.setAttribute('role','region');wrap.setAttribute('aria-label','Chapter referral statistics; scroll horizontally for all columns');
    const table=node('table',undefined,'statistics-table');table.setAttribute('aria-label','Signups by referring chapter');
    const head=node('thead'),header=node('tr');for(const label of ['Referring chapter',...columns.map(c=>c[1])]){const th=node('th',label);th.scope='col';header.append(th)}head.append(header);
    const body=node('tbody');
    const addRow=(target,label,values)=>{const tr=node('tr'),name=node('th',label);name.scope='row';tr.append(name);for(const [key] of columns)tr.append(node('td',String(values[key])));target.append(tr)};
    for(const row of statistics.chapters){
      const label=row.chapter_id===null?'No chapter code':row.chapter_name?`${row.chapter_name}${row.chapter_code?' ('+row.chapter_code+')':''}`:`Chapter ${row.chapter_id}`;
      addRow(body,label,row);
    }
    const foot=node('tfoot');addRow(foot,'Total',statistics.totals);table.append(head,body,foot);wrap.append(table);results.append(wrap);results.hidden=false;
    status.textContent=statistics.totals.signups?`Updated ${new Date(statistics.as_of_utc).toLocaleString('en-US',{timeZone:eventData.time_zone,timeZoneName:'short'})}.`:'No registrations for this selection yet.';
  }catch(error){if(version===statisticsRequest){results.hidden=true;status.textContent=error.status===401||error.status===403?'You need signup-management permission to view statistics.':`Couldn’t load statistics. ${error.message}`}}
  finally{if(version===statisticsRequest)button.disabled=false}
}
document.getElementById('statistics-date').addEventListener('change',loadWorkshopStatistics);
document.getElementById('statistics-refresh').addEventListener('click',loadWorkshopStatistics);

function workshopCancelButton(item){
  const button=action('Cancel',async()=>{
    if(!confirm(`Cancel ${item.title}? It will be removed from public registration. Existing signups will be cancelled; notification emails will be prepared and sent when email delivery is enabled. Registration history will be kept.`))return;
    await adminWrite(adminBase(item.occurrence_id),'DELETE');await loadManaged();announce('Workshop cancelled. Registration history has been kept.');document.getElementById(`workshop-toggle-${item.occurrence_id}`)?.focus();
  });button.className='cancel-workshop';button.setAttribute('aria-label',`Cancel workshop: ${item.title}`);return button;
}

function addWorkshopControls(card,item){
  const toolbar=node('div',undefined,'actions');
  const roster=node('section');roster.hidden=true;
  const meeting=node('section');meeting.hidden=true;
  const clock=node('section');clock.hidden=true;
  if(item.lifecycle==='published'){
    const toggle=action('Rehearsal clock',async()=>{clock.hidden=!clock.hidden;if(!clock.hidden)await loadRehearsalClock(clock,item)});
    toggle.className='rehearsal-clock-toggle';toggle.hidden=!workshopIsRehearsal;toolbar.append(toggle);
  }
  toolbar.append(action('Zoom meeting',async()=>{meeting.hidden=!meeting.hidden;if(!meeting.hidden)await loadMeeting(meeting,item)}));
  toolbar.append(action('Signups & attendance',async()=>{roster.hidden=!roster.hidden;if(!roster.hidden)await loadRoster(roster,item)}));
  card.append(toolbar,clock,meeting,roster);
}

async function loadRehearsalClock(container,item){
  const url=`${adminBase(item.occurrence_id)}/rehearsal-clock`;
  const data=await jsonRequest(url);const clock=data.clock;
  const stages=['Registration','24-hour reminder','1-hour reminder','Attendance','Follow-up'];
  container.replaceChildren(node('h3','Rehearsal clock'),node('p',`Current step: ${stages[clock.stage]||'Not started'}. Simulated time: ${workshopDate(clock.simulated_at_utc)}.`));
  container.append(node('p','The clock stays at this step until you advance it. Review the emails between steps. At Attendance, open Signups & attendance and record results before advancing to Follow-up.'));
  if(clock.pending)container.append(node('p',`${clock.pending} email(s) still need to finish at this step. Check Email status in the signup list, then refresh the clock.`));
  const controls=actionGroup(action('Refresh rehearsal clock',()=>loadRehearsalClock(container,item)));container.append(controls);
  if(clock.stage<4){
    const advance=action(clock.stage<0?'Start rehearsal clock':`Advance to ${stages[clock.stage+1]}`,async()=>{
      await adminWrite(url,'POST',{expected_stage:clock.stage});await loadRehearsalClock(container,item);
    });advance.disabled=clock.stage>=0&&clock.pending>0;controls.append(advance);
  }else container.append(node('p','Follow-up step reached. Unrecorded attendance still holds that participant’s message. Record attendance when ready; sent messages will not be repeated.'));
}

async function loadMeeting(container,item){
  const result=await jsonRequest(`${adminBase(item.occurrence_id)}/meeting`);const meeting=result.meeting;
  container.replaceChildren(node('h3','Zoom meeting'),node('p',`Status: ${meeting.sync_status}${meeting.sync_error?' · '+meeting.sync_error:''}`));
  if(meeting.join_url){const link=node('a','Participant joining link');link.href=meeting.join_url;link.target='_blank';link.rel='noopener';container.append(link)}
  if(meeting.meeting_id)container.append(node('p',`Meeting ID: ${meeting.meeting_id}`));
  const controls=actionGroup(action('Refresh Zoom status',()=>loadMeeting(container,item)));container.append(controls);
  if(meeting.sync_status==='uncertain'){
    container.append(node('p','Inspect the AGA Zoom account before continuing. An operation may have succeeded even though its response was lost.'));
    const form=node('form');form.append(field('Existing meeting ID (leave blank only if you verified there is no meeting)','meeting_id'),field('What did you verify in Zoom?','note',{required:true}));
    const submit=node('button','Record verification');submit.type='submit';form.append(submit);
    form.addEventListener('submit',async e=>{e.preventDefault();submit.disabled=true;try{
      const payload=Object.fromEntries(new FormData(form));payload.confirmed_absent=!payload.meeting_id.trim();
      if(payload.confirmed_absent&&!confirm('Have you checked the AGA Zoom account and verified that no meeting exists for this workshop?'))return;
      await adminWrite(`${adminBase(item.occurrence_id)}/meeting`,'PATCH',payload);await loadMeeting(container,item);
    }catch(error){showError(error.message)}finally{submit.disabled=false}});container.append(form);return;
  }
  if(item.lifecycle!=='published'||new Date(item.starts_at_utc)<=new Date()||meeting.sync_status==='working')return;
  controls.append(action(meeting.meeting_id?'Sync Zoom meeting':'Schedule with AGA Zoom',async()=>{
    await adminWrite(`${adminBase(item.occurrence_id)}/meeting`,'PUT',{source:'zoom'});await loadMeeting(container,item);
  }));
  if(!meeting.meeting_id){
    const form=node('form');form.append(field('Or paste a participant Zoom joining link','join_url',{type:'url',required:true}));
    const submit=node('button','Save manual link');submit.type='submit';form.append(submit);
    form.addEventListener('submit',async e=>{e.preventDefault();submit.disabled=true;try{
      await adminWrite(`${adminBase(item.occurrence_id)}/meeting`,'PUT',{source:'manual',join_url:new FormData(form).get('join_url')});await loadMeeting(container,item);
    }catch(error){showError(error.message)}finally{submit.disabled=false}});container.append(form);
  }
}

async function loadRoster(container,item){
  const data=await jsonRequest(`${adminBase(item.occurrence_id)}/roster`);container.replaceChildren();
  container.append(node('h3',`Signups · ${item.title}`));
  container.append(node('p','Record participants’ OGS handles here when they reply to the confirmation email.','muted'));
  container.append(node('p','After the workshop, mark each confirmed participant attended or absent. Changes save automatically. Attended sends your edited follow-up; absent invites them to another free workshop. Unrecorded holds the follow-up until you record attendance.','muted'));
  if(!data.roster.length)container.append(node('p','No signups yet.'));
  for(const person of data.roster){
    const row=node('div',undefined,'panel');
    row.append(node('strong',`${person.first_name} ${person.last_name}`),node('div',`${person.email} · ${person.status}`));
    const handleForm=node('form',undefined,'ogs-handle-form');
    const handleField=field('OGS handle','ogs_handle',{maxlength:100});
    const handleInput=handleField.querySelector('input');handleInput.value=person.ogs_handle||'';handleInput.placeholder='Not provided yet';handleInput.setAttribute('aria-label',`OGS handle for ${person.first_name} ${person.last_name}`);
    const saveHandle=node('button','Save OGS handle');saveHandle.type='submit';
    const handleStatus=node('span',undefined,'status');handleStatus.setAttribute('role','status');
    handleForm.append(handleField,saveHandle,handleStatus);
    handleForm.addEventListener('submit',async e=>{e.preventDefault();clearError();saveHandle.disabled=true;handleStatus.textContent='';try{
      const result=await adminWrite(`${adminBase(item.occurrence_id)}/roster/${person.registration_occurrence_id}`,'PATCH',{ogs_handle:handleInput.value});
      person.ogs_handle=result.ogs_handle;handleInput.value=result.ogs_handle||'';handleStatus.textContent=result.ogs_handle?'OGS handle saved.':'OGS handle cleared.';
    }catch(error){showError(error.message)}finally{saveHandle.disabled=false}});row.append(handleForm);
    if(person.status==='confirmed'){
      const label=node('label','Attendance ');const select=node('select');select.setAttribute('aria-label',`Attendance for ${person.first_name} ${person.last_name}`);
      for(const value of ['unrecorded','attended','absent']){const option=node('option',value);option.value=value;select.append(option)}select.value=person.attendance;
      const saved=person.attendance;select.addEventListener('change',async()=>{select.disabled=true;try{
        await adminWrite(`${adminBase(item.occurrence_id)}/roster/${person.registration_occurrence_id}`,'PATCH',{attendance:select.value});await loadRoster(container,item)
      }catch(e){select.value=saved;showError(e.message)}finally{select.disabled=false}});label.append(select);row.append(label);
    }
    if(person.status!=='cancelled')row.append(actionGroup(action('Cancel signup',async()=>{
      if(!confirm(`Cancel the signup for ${person.first_name} ${person.last_name}? The next eligible waitlisted signup will be promoted.`))return;
      await adminWrite(`${adminBase(item.occurrence_id)}/roster/${person.registration_occurrence_id}`,'DELETE');await loadRoster(container,item);
    })));
    const messages=data.emails.filter(m=>m.registration_id===person.registration_id);
    const details=node('details');details.append(node('summary','Email status'));
    for(const message of messages){
      const line=node('div',`${message.kind}: ${message.status}${message.sent_at_utc?' · '+new Date(message.sent_at_utc).toLocaleString():''}${message.last_error?' · '+message.last_error:''}`);
      const messageActions=actionGroup();
      if(['failed','uncertain'].includes(message.status))for(const [label,resolution] of [['Verified sent','sent'],['Verified not sent — retry','retry']])messageActions.append(action(label,async()=>{
        const note=prompt('Check the sender’s Sent folder for Message-ID '+(workshopIsRehearsal?'test-':'')+'aga-workshop-email-'+message.email_id+'@usgo.org. Describe what you verified:');if(!note?.trim())return;
        await adminWrite(`/api/events/manage/${eventData.event_id}/emails/${message.email_id}`,'PATCH',{resolution,note});await loadRoster(container,item);
      }));if(messageActions.childElementCount)line.append(messageActions);details.append(line);
    }
    row.append(details);container.append(row);
  }
  if(item.lifecycle==='published'&&new Date(item.starts_at_utc)>new Date()){
    const form=node('form',undefined,'panel');form.append(node('h3','Add signup'));
    const fields=node('div',undefined,'fields');
    for(const [label,name] of [['First name','first_name'],['Last name','last_name'],['Email','email']])fields.append(field(label,name,{required:true,type:name==='email'?'email':'text',maxlength:name==='email'?254:100,wide:name==='email'}));
    for(const [label,name,maxlength] of [['City','city',120],['State or province','state_province',120],['Country','country',100],['ZIP or postal code','postal_code',32]])fields.append(field(`${label} (optional)`,name,{maxlength}));
    form.append(fields);
    const confirmationLabel=node('label',undefined,'signup-notification');
    const confirmation=node('input');confirmation.type='checkbox';confirmation.name='send_confirmation_email';confirmation.checked=true;
    confirmationLabel.append(confirmation,document.createTextNode('Send confirmation email'));form.append(confirmationLabel);
    const submit=node('button','Add signup');submit.type='submit';form.append(actionGroup(submit));
    let key=crypto.randomUUID();form.addEventListener('submit',async e=>{e.preventDefault();submit.disabled=true;try{
      const data=new FormData(form);data.delete('send_confirmation_email');const contact=Object.fromEntries(data);await adminWrite(`${adminBase(item.occurrence_id)}/roster`,'POST',{contact,idempotency_key:key,send_confirmation_email:confirmation.checked});key=crypto.randomUUID();await loadRoster(container,item);
    }catch(error){showError(error.message)}finally{submit.disabled=false}});container.append(form);
  }
}

function renderAttendedPreview(container,body,instructor){
  const heading='Subject: Continue learning Go: [workshop title]\n\nHi [first name],\n\n';
  if(!body.includes('**')&&!/^- /m.test(body)){container.textContent=`${heading}${body.trim()}\n\nBest,\n${instructor.trim()}`;return}
  container.replaceChildren(node('div',heading));
  function inline(target,text){for(const part of text.split(/(\*\*[^*\n]+\*\*)/))target.append(part.startsWith('**')&&part.endsWith('**')?node('strong',part.slice(2,-2)):document.createTextNode(part))}
  for(const block of body.trim().split(/\n\s*\n/)){
    const lines=block.split('\n');
    if(lines.every(line=>line.startsWith('- '))){const list=node('ul');for(const line of lines){const item=node('li');inline(item,line.slice(2));list.append(item)}container.append(list)}
    else{const paragraph=node('p');inline(paragraph,block);container.append(paragraph)}
  }
  container.append(node('p',`Best,\n${instructor.trim()}`));
}

async function loadWorkshopSettings(){
  const target=document.getElementById('workshop-settings');
  const data=await jsonRequest(`/api/events/manage/${eventData.event_id}/settings`);
  const settings=data.settings;target.replaceChildren();
  workshopIsRehearsal=!!settings.is_test;
  document.querySelectorAll('.rehearsal-clock-toggle').forEach(button=>button.hidden=!workshopIsRehearsal);
  target.append(node('p','Default reminders are 24 hours and 1 hour before the workshop. Follow-ups are due after the delay below: attended participants receive your message; absent participants receive an invitation to register for another free workshop. Unrecorded attendance holds the follow-up. Email delivery must also be enabled on the server.','muted'));
  const form=node('form');
  for(const [label,name,type] of [['Sender email','sender_email','email'],['Instructor','instructor_name','text'],['Licensed AGA Zoom host email','zoom_host_email','email'],['Reminder hours, separated by commas','reminder_hours','text'],['Follow-up delay (hours)','followup_hours','number']]){
    const f=field(label,name,{type,required:name!=='zoom_host_email'&&name!=='reminder_hours'});f.querySelector('input').value=name==='reminder_hours'?settings[name].join(', '):settings[name]??'';form.append(f);
  }
  const label=node('label','Attended follow-up email');const textarea=node('textarea');textarea.name='followup_text';textarea.required=true;textarea.maxLength=4000;textarea.rows=10;textarea.value=settings.followup_text;label.append(textarea);form.append(label);
  form.append(node('p','Write the message body, including any resource or course links. Use **bold text** for emphasis and lines starting with - for bullets. The first-name greeting and instructor signature are added automatically. Saving applies to future unsent follow-ups across this workshop series.','muted'));
  const preview=node('details');preview.append(node('summary','Preview attended email'));
  const previewBody=node('div');previewBody.style.whiteSpace='pre-wrap';previewBody.style.overflowWrap='anywhere';preview.append(previewBody);form.append(preview);
  const instructor=form.elements.namedItem('instructor_name');
  const updatePreview=()=>renderAttendedPreview(previewBody,textarea.value,instructor.value);
  textarea.addEventListener('input',updatePreview);instructor.addEventListener('input',updatePreview);updatePreview();
  const enableLabel=node('label',' Enable email delivery for this workshop series ');const enable=node('input');enable.type='checkbox';enable.name='email_enabled';enable.checked=settings.email_enabled;enableLabel.prepend(enable);form.append(enableLabel);
  const submit=node('button','Save settings');submit.type='submit';const status=node('p');status.setAttribute('role','status');form.append(submit,status);
  form.addEventListener('submit',async e=>{e.preventDefault();submit.disabled=true;try{
    const payload=Object.fromEntries(new FormData(form));payload.reminder_hours=payload.reminder_hours.trim()?payload.reminder_hours.split(',').map(s=>Number(s.trim())):[];
    payload.followup_hours=Number(payload.followup_hours);payload.email_enabled=enable.checked;
    await adminWrite(`/api/events/manage/${eventData.event_id}/settings`,'PUT',payload);status.textContent='Settings saved.';
  }catch(error){showError(error.message)}finally{submit.disabled=false}});target.append(form);
}
