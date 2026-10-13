// Headless UI regression test. All network requests are fulfilled locally.
const fs=require('fs');
const path=require('path');
const assert=require('assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
const html=fs.readFileSync(path.join(root,'event_manage.html'),'utf8');
const script=fs.readFileSync(path.join(root,'workshop_admin.js'),'utf8');

(async()=>{
 const browser=await chromium.launch({headless:true,channel:'chrome'});
 try{
  for(const width of [1280,390]){
   const page=await browser.newPage({viewport:{width,height:1000}});
   const errors=[];page.on('pageerror',e=>errors.push(e.message));
   const start=new Date(Date.now()+48*3600*1000);
   const item={occurrence_id:1,occurrence_key:'test-workshop',title:'Free 1-hour workshop',starts_at_utc:start.toISOString(),ends_at_utc:new Date(+start+3600*1000).toISOString(),registration_closes_at_utc:start.toISOString(),capacity:10,waitlist_enabled:true,lifecycle:'published',confirmed_count:1,waitlisted_count:0,location_name:'Online via Zoom'};
   const past={...item,occurrence_id:2,occurrence_key:'past-workshop',title:'Past workshop',starts_at_utc:new Date(Date.now()-48*3600*1000).toISOString(),ends_at_utc:new Date(Date.now()-47*3600*1000).toISOString()};
   const items=[item,past];const event={event_id:1,name:'Free 1-hour workshop',time_zone:'America/New_York',occurrences:items};
   const settings={sender_email:'milan.mladenovic@usgo.org',instructor_name:'Milan',reminder_hours:[24,1],followup_hours:24,followup_text:'Keep playing Go.',email_enabled:false,zoom_host_email:''};
   const person={registration_occurrence_id:5,registration_id:'r1',first_name:'Ada',last_name:'Lovelace',email:'ada@example.com',status:'confirmed',attendance:'unrecorded'};
   const roster=[person];const requests=[];
   let clockStage=0,meetingReady=false;
   await page.route('https://workshop.test/**',async route=>{
    const req=route.request();const url=new URL(req.url());const method=req.method();let body;
    if(!url.pathname.includes('/api/'))return route.fulfill({contentType:url.pathname.endsWith('.js')?'text/javascript':'text/html',body:url.pathname.endsWith('.js')?script:html});
    if(method!=='GET'){assert.equal(req.headers()['x-events-admin'],'1');requests.push({path:url.pathname,method,body:req.postDataJSON()})}
    if(url.pathname.endsWith('/rehearsal-clock')){if(method==='POST'&&req.postDataJSON().expected_stage===clockStage)clockStage++;body={ok:true,clock:{stage:clockStage,simulated_at_utc:start.toISOString(),pending:0}};}
    else if(url.pathname.endsWith('/settings')){if(method==='PUT')Object.assign(settings,req.postDataJSON());body={ok:true,settings,delivery:{mode:'test',test_recipient:'gurujeet.khalsa@usgo.org',enabled:true}}}
    else if(url.pathname.endsWith('/meeting'))body={ok:true,meeting:{sync_status:meetingReady?'ready':'not_configured',join_url:meetingReady?'https://zoom.us/j/123456789':null,meeting_id:meetingReady?'123456789':null}};
    else if(/\/roster\/5$/.test(url.pathname)){
     if(method==='DELETE')person.status='cancelled';else Object.assign(person,req.postDataJSON());
     body={ok:true,ogs_handle:person.ogs_handle||null};
    }else if(url.pathname.endsWith('/roster')){
     if(method==='POST')roster.push({...req.postDataJSON().contact,registration_id:'r2',registration_occurrence_id:6,status:'confirmed',attendance:'unrecorded'});
     body={ok:true,roster,emails:[],occurrence:item};
    }else if(url.pathname.endsWith('/occurrences')){
     if(method==='POST'){const payload=req.postDataJSON();assert(payload.occurrence_key);const added={...item,...payload,occurrence_id:3,starts_at_utc:start.toISOString(),ends_at_utc:new Date(+start+3600*1000).toISOString()};items.push(added);body={ok:true,occurrence:added}}
     else body={ok:true,event,occurrences:items};
    }else if(method==='DELETE'){const id=Number(url.pathname.split('/').pop());items.find(i=>i.occurrence_id===id).lifecycle='cancelled';body={ok:true};}
    else if(method==='PATCH'){const id=Number(url.pathname.split('/').pop());const edited=items.find(i=>i.occurrence_id===id);Object.assign(edited,req.postDataJSON());body={ok:true,occurrence:edited};}
    else body={ok:true,event};
    await route.fulfill({contentType:'application/json',body:JSON.stringify(body)});
   });
   await page.goto('https://workshop.test/events/manage/learn-go-in-an-hour');
   await page.locator('#manager').waitFor({state:'visible'});
   await page.locator('#email-delivery-banner').waitFor();
   assert((await page.locator('#email-delivery-banner').innerText()).includes('Emails for this real series are paused during testing.'));
   assert.equal(await page.locator('#add-form').getByLabel('Title',{exact:true}).inputValue(),'Free Beginners 1-Hour Workshop');
   await page.locator('#qr-downloads').waitFor({state:'visible'});
   await page.locator('#qr-downloads > summary').click();
   assert.equal(await page.getByRole('link',{name:'Download PNG',exact:true}).getAttribute('href'),'/events/learn-go-in-an-hour/workshop-qr/png');
   assert.equal(await page.getByRole('link',{name:'Download SVG',exact:true}).getAttribute('download'),'free-beginners-workshop-qr.svg');
   await page.locator('#qr-downloads > summary').click();
   assert.equal(await page.locator('#occurrences article').count(),2);
   const addButton=page.getByRole('button',{name:'Add Workshop',exact:true});
   assert((await addButton.boundingBox()).y<(await page.locator('#occurrences').boundingBox()).y);
   assert.equal(await page.locator('.occurrence-detail:visible').count(),0);
   assert.equal(await page.getByRole('button',{name:'Cancel workshop: Free 1-hour workshop',exact:true}).isVisible(),true);
   assert.equal(await page.locator('#add-workshop-panel').isVisible(),false);
   fs.mkdirSync(path.join(root,'test-results'),{recursive:true});
   await page.screenshot({path:path.join(root,`test-results/admin-summary-${width}.png`),fullPage:true});
   await page.getByText('Email & Zoom settings',{exact:true}).click();
   await page.getByLabel('Sender email',{exact:true}).fill('new@usgo.org');
   const attendedText='Thanks for joining us!\n\nPractice: https://www.usgo.org/';
   await page.getByLabel('Attended follow-up email',{exact:true}).fill(attendedText);
   await page.getByText('Preview attended email',{exact:true}).click();
   const preview=page.locator('#workshop-settings details');
   assert((await preview.innerText()).includes('Hi [first name],\n\n'+attendedText+'\n\nBest,\nMilan'));
   await page.getByRole('button',{name:'Save settings',exact:true}).click();
   await page.getByText('Settings saved.',{exact:true}).waitFor();assert.equal(settings.sender_email,'new@usgo.org');
   assert.equal(settings.followup_text,attendedText);assert.equal(settings.email_enabled,false);
   await page.reload();await page.getByText('Email & Zoom settings',{exact:true}).click();
   assert.equal(await page.getByLabel('Attended follow-up email',{exact:true}).inputValue(),attendedText);
   await page.getByText('Preview attended email',{exact:true}).click();
   await page.screenshot({path:path.join(root,`test-results/attended-email-${width}.png`),fullPage:true});
   const first=page.locator('#occurrences article').first();
   await first.locator('.occurrence-toggle').focus();await page.keyboard.press('Enter');
   assert.equal(await first.locator('.occurrence-detail').isVisible(),true);
   assert.equal(await first.locator('.occurrence-toggle').getAttribute('aria-expanded'),'true');
   assert.equal(await first.getByRole('button',{name:'Rehearsal clock',exact:true}).isVisible(),false);
   settings.is_test=true;await page.reload();
   await first.locator('.occurrence-toggle').click();
   await first.getByRole('button',{name:'Rehearsal clock',exact:true}).click();
   for(const step of ['24-hour reminder','1-hour reminder','Attendance']){
    await first.getByRole('button',{name:`Advance to ${step}`,exact:true}).click();
   }
   assert.equal(clockStage,3);
   await first.getByRole('button',{name:'Refresh rehearsal clock',exact:true}).click();assert.equal(clockStage,3);
   await first.getByRole('button',{name:'Advance to Follow-up',exact:true}).waitFor();
   await page.screenshot({path:path.join(root,`test-results/rehearsal-clock-${width}.png`),fullPage:true});
   await first.getByLabel('Title',{exact:true}).fill('Updated workshop');
   await first.getByRole('button',{name:'Save changes',exact:true}).click();
   await page.getByText('Changes saved.',{exact:true}).waitFor();
   assert.equal(await first.locator('.occurrence-detail').isVisible(),true);
   assert.equal(await first.getByLabel('Title',{exact:true}).inputValue(),'Updated workshop');
   await first.locator('.occurrence-toggle').click();assert.equal(await first.locator('.occurrence-detail').isVisible(),false);
   await first.locator('.occurrence-toggle').click();
   await first.getByRole('button',{name:'Signups & attendance'}).click();
   await first.getByText('Ada Lovelace',{exact:true}).waitFor();
   const handleInput=first.getByLabel('OGS handle for Ada Lovelace',{exact:true});
   assert.equal(await handleInput.getAttribute('placeholder'),'Not provided yet');
   await handleInput.fill('AdaGo');await first.getByRole('button',{name:'Save OGS handle',exact:true}).click();
   await first.getByText('OGS handle saved.',{exact:true}).waitFor();
   const oldHandle=await handleInput.elementHandle();await first.getByRole('button',{name:'Signups & attendance'}).click();await first.getByRole('button',{name:'Signups & attendance'}).click();await page.waitForFunction(input=>!input.isConnected,oldHandle);
   await handleInput.waitFor();assert.equal(await handleInput.inputValue(),'AdaGo');
   await handleInput.fill('');await first.getByRole('button',{name:'Save OGS handle',exact:true}).click();
   await first.getByText('OGS handle cleared.',{exact:true}).waitFor();
   const form=first.locator('form').filter({has:page.getByRole('heading',{name:'Add signup'})});
   await form.getByLabel('First name',{exact:true}).fill('Grace');await form.getByLabel('Last name',{exact:true}).fill('Hopper');await form.getByLabel('Email',{exact:true}).fill('grace@example.com');
   await form.getByRole('button',{name:'Add signup',exact:true}).click();await first.getByText('Grace Hopper',{exact:true}).waitFor();
   const pastCard=page.locator('#occurrences article').nth(1);
   await pastCard.locator('.occurrence-toggle').click();
   assert.equal(await pastCard.getByRole('button',{name:'Save changes',exact:true}).count(),0);
   await pastCard.getByRole('button',{name:'Signups & attendance'}).click();
   await Promise.all([page.waitForResponse(r=>r.url().endsWith('/roster/5')&&r.request().method()==='PATCH'),pastCard.getByLabel('Attendance for Ada Lovelace').selectOption('attended')]);
   assert(requests.some(r=>r.method==='PATCH'&&r.body.attendance==='attended'));
   page.once('dialog',d=>d.accept());
   await first.getByRole('button',{name:'Cancel signup',exact:true}).first().click();await first.getByText('ada@example.com · cancelled',{exact:true}).waitFor();
   await first.getByRole('button',{name:'Zoom meeting',exact:true}).click();await first.getByText('Status: not_configured',{exact:true}).waitFor();
   meetingReady=true;await first.getByRole('button',{name:'Refresh Zoom Link',exact:true}).click();await page.waitForFunction(()=>document.querySelector('[data-occurrence-id="1"] [name=zoom_join_url]').value==='https://zoom.us/j/123456789');assert.equal(await first.getByLabel('Zoom Link',{exact:true}).getAttribute('readonly'),'');
   const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth);assert.equal(overflow,false);
   await page.screenshot({path:path.join(root,`test-results/admin-${width}.png`),fullPage:true});
   await addButton.click();assert.equal(await page.locator('#add-workshop-panel').isVisible(),true);
   const addForm=page.locator('#add-form');assert.equal(await addForm.getByLabel('Title',{exact:true}).inputValue(),'Test - Free Beginners 1-Hour Workshop');assert.equal(await addForm.getByLabel('Time (Eastern)',{exact:true}).inputValue(),'18:00');assert.equal(await addForm.getByLabel('Zoom Link',{exact:true}).getAttribute('readonly'),'');assert.equal(await addForm.locator('[name=ends_at_local], [name=registration_opens_at_local], [name=registration_closes_at_local], [name=location_name]').count(),0);await addForm.getByLabel('Title',{exact:true}).fill('A newly added workshop');
   await addForm.getByLabel('Date',{exact:true}).fill('2027-03-11');await page.screenshot({path:path.join(root,`test-results/manager-fields-${width}.png`),fullPage:true});
   await addForm.getByRole('button',{name:'Add workshop',exact:true}).click();await page.getByText('Workshop added.',{exact:true}).waitFor();
   assert.equal(requests.find(r=>r.method==='POST'&&r.path.endsWith('/occurrences')).body.starts_at_local,'2027-03-11T18:00');assert.equal(await page.locator('#add-workshop-panel').isVisible(),false);assert.equal(await page.locator('#occurrences article').count(),3);
   assert.equal(await page.locator('[data-occurrence-id="3"] .occurrence-detail').isVisible(),true);
   await first.locator('.occurrence-toggle').click();assert.equal(await first.locator('.occurrence-detail').isVisible(),false);
   const cancelButton=first.getByRole('button',{name:'Cancel workshop: Updated workshop',exact:true});
   const deleteCount=requests.filter(r=>r.method==='DELETE').length;
   page.once('dialog',d=>d.dismiss());await cancelButton.click();assert.equal(requests.filter(r=>r.method==='DELETE').length,deleteCount);
   assert.equal(await first.locator('.occurrence-detail').isVisible(),false);
   await addButton.click();await page.locator('#add-form').getByLabel('Title',{exact:true}).fill('Keep this draft');
   page.once('dialog',d=>{assert(d.message().includes('Registration history will be kept'));d.accept()});
   await cancelButton.click();await page.getByText('Workshop cancelled. Registration history has been kept.',{exact:true}).waitFor();
   assert.equal(await first.locator('.workshop-status').innerText(),'Cancelled');assert.equal(await first.getByRole('button',{name:/Cancel workshop:/}).count(),0);
   assert.equal(await first.locator('.occurrence-detail').isVisible(),false);assert.equal(await page.locator('#add-form').getByLabel('Title',{exact:true}).inputValue(),'Keep this draft');
   assert.equal(items[1].lifecycle,'published');assert.equal(items[2].lifecycle,'published');
   assert.deepEqual(errors,[]);console.log(`Admin UI ${width}px: compact summaries, keyboard disclosure, add/edit, guarded cancellation, draft preservation, settings, roster, attendance, Zoom passed.`);
   await page.close();
  }
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
