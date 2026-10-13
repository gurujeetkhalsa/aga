const {chromium}=require('playwright');
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
const html=fs.readFileSync(path.join(__dirname,'../event_registration.html'),'utf8');

(async()=>{
  const browser=await chromium.launch({headless:true,channel:'chrome'});
  try {
    for(const width of [1280,390]) for(const scenario of ['only-waitlist','single','multiple']) {
      const page=await browser.newPage({viewport:{width,height:844}});
      const errors=[],submissions=[];
      let reject=true;
      const dates=[{occurrence_id:11,title:'Full workshop',capacity:10,confirmed_count:10,remaining_capacity:0,waitlist_enabled:true,selectable:true,availability:'waitlist'}];
      if(scenario!=='only-waitlist') dates.push({occurrence_id:12,title:'Available workshop',capacity:10,confirmed_count:1,remaining_capacity:9,waitlist_enabled:true,selectable:true,availability:'available'});
      dates.push({occurrence_id:13,title:'Closed workshop',capacity:10,confirmed_count:10,remaining_capacity:0,waitlist_enabled:false,selectable:false,availability:'full'});
      dates.forEach(d=>Object.assign(d,{starts_at_utc:'2026-09-17T22:00:00Z',ends_at_utc:'2026-09-17T23:00:00Z'}));
      page.on('pageerror',e=>errors.push(e.message));
      await page.route('https://workshop.test/**',async route=>{
        if(route.request().method()==='POST') {
          const payload=route.request().postDataJSON();submissions.push(payload);
          if(reject) return route.fulfill({status:400,json:{ok:false,error:{message:'Test retry message'}}});
          return route.fulfill({json:{ok:true,registration:{registration_id:123,status:'waitlisted',occurrences:[{title:'Full workshop',status:'waitlisted'}]}}});
        }
        if(route.request().url().includes('/api/')) return route.fulfill({json:{ok:true,event:{name:'Rehearsal',time_zone:'America/New_York',selection_mode:scenario==='multiple'?'multiple':'single',public_occurrence_limit:4,occurrences:dates}}});
        return route.fulfill({contentType:'text/html',body:html});
      });
      await page.goto('https://workshop.test/events/rehearsal');
      await page.locator('#content').waitFor({state:'visible'});
      const button=page.locator('#submit'),waitlist=page.locator('[value="11"]');
      assert.equal(await button.innerText(),scenario==='only-waitlist'?'Join Waitlist':'Register');
      assert((await page.locator('#occurrences').innerText()).includes('Waitlist available'));
      assert(!/join waitlist/i.test(await page.locator('#occurrences').innerText()));
      assert(await page.locator('[value="13"]').isDisabled());
      assert.equal(submissions.length,0);
      await waitlist.check();
      assert.equal(await button.innerText(),'Join Waitlist');
      if(scenario!=='only-waitlist') {
        await page.locator('[value="12"]').check();
        assert.equal(await button.innerText(),scenario==='multiple'?'Register & Join Waitlist':'Register');
        if(scenario==='multiple') {
          await waitlist.uncheck();assert.equal(await button.innerText(),'Register');
          await page.locator('[value="12"]').uncheck();assert.equal(await button.innerText(),'Register');
        }
        await waitlist.check();assert.equal(await button.innerText(),'Join Waitlist');
      }
      for(const [name,value] of Object.entries({first_name:'Test',last_name:'Person',email:'test@example.com'})) await page.locator(`[name="${name}"]`).fill(value);
      await button.click();
      await page.locator('#error').waitFor({state:'visible'});
      assert.equal(await button.innerText(),'Join Waitlist');
      assert(await button.isEnabled());
      assert.deepEqual(submissions[0].occurrence_ids,[11]);
      reject=false;
      await button.click();await page.locator('#confirmation').waitFor({state:'visible'});
      assert.equal(await page.locator('#confirmation h2').innerText(),'You’re on the waitlist');
      assert.equal(submissions.length,2);assert.deepEqual(errors,[]);
      console.log(JSON.stringify({width,scenario,passed:true,liveSubmissions:0}));
      await page.close();
    }
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
