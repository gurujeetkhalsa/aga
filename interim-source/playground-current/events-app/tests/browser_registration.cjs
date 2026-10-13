const {chromium} = require('playwright');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const html = fs.readFileSync(path.join(__dirname,'../event_registration.html'),'utf8');

(async()=>{
  const browser=await chromium.launch({headless:true,channel:'chrome'});
  try {
    for(const width of [1280,390]) for(const mode of ['single','multiple']) {
      const page=await browser.newPage({viewport:{width,height:844}});
      const errors=[],submissions=[];
      page.on('pageerror',e=>errors.push(e.message));
      await page.route('https://workshop.test/**',async route=>{
        if(route.request().method()==='POST') {
          submissions.push(route.request().postDataJSON());
          return route.fulfill({json:{ok:true,registration:{registration_id:123,status:'confirmed',occurrences:[{title:'Test workshop',status:'confirmed'}]}}});
        }
        if(route.request().url().includes('/api/')) return route.fulfill({json:{ok:true,event:{name:'Workshop rehearsal',time_zone:'America/New_York',selection_mode:mode,public_occurrence_limit:4,occurrences:[{occurrence_id:11,title:'Test workshop',starts_at_utc:'2026-09-17T22:00:00Z',ends_at_utc:'2026-09-17T23:00:00Z',capacity:10,confirmed_count:1,remaining_capacity:9,selectable:true,availability:'available'}]}}});
        return route.fulfill({contentType:'text/html',body:html});
      });
      await page.goto('https://workshop.test/events/learn-go-in-an-hour-rehearsal'+(width===390?'?ref=22385':''));
      await page.locator('#content').waitFor({state:'visible'});
      // Even an empty contact form must explain the missing date first.
      await page.locator('#submit').click();
      assert(await page.locator('#selection-error').isVisible());
      assert.equal(submissions.length,0);
      for(const [name,value] of Object.entries({first_name:'Joe',last_name:'Jones',email:'joe@example.com'})) await page.locator(`[name="${name}"]`).fill(value);
      await page.locator('#submit').click();
      assert(await page.locator('#selection-error').isVisible());
      assert.equal(await page.evaluate(()=>document.activeElement.id),'selection-error');
      const box=await page.locator('#selection-error').boundingBox();
      assert(box.y>=0 && box.y+box.height<=844,'Feedback must be inside the viewport');
      assert.equal(await page.locator('[name="occurrence"]').getAttribute('aria-invalid'),'true');
      await page.locator('#choose-date').click();
      assert.equal(await page.evaluate(()=>document.activeElement.name),'occurrence');
      await page.keyboard.press('Space');
      assert(await page.locator('[name="occurrence"]').isChecked());
      assert(await page.locator('#selection-error').isHidden());
      assert.equal(await page.locator('[name="occurrence"]').getAttribute('aria-invalid'),null);
      assert.equal(await page.locator('[name="first_name"]').inputValue(),'Joe');
      // Native required/email validation still blocks invalid contact data.
      await page.locator('[name="email"]').fill('invalid');
      await page.locator('#submit').click();
      assert.equal(submissions.length,0);
      assert.equal(await page.evaluate(()=>document.activeElement.name),'email');
      await page.locator('[name="email"]').fill('joe@example.com');
      await page.locator('[name="last_name"]').fill('');
      await page.locator('#submit').click();
      assert.equal(submissions.length,0);
      assert.equal(await page.evaluate(()=>document.activeElement.name),'last_name');
      await page.locator('[name="last_name"]').fill('Jones');
      await page.keyboard.press('Enter');
      await page.locator('#confirmation').waitFor({state:'visible'});
      assert.equal(submissions.length,1);
      assert.equal(submissions[0].referral_chapter_id,width===390?'22385':null);
      assert.deepEqual(submissions[0],{occurrence_ids:[11],referral_chapter_id:width===390?'22385':null,contact:{first_name:'Joe',last_name:'Jones',email:'joe@example.com',city:'',state_province:'',country:'',postal_code:''}});
      assert.deepEqual(errors,[]);
      console.log(JSON.stringify({width,mode,passed:true,liveRegistrationsCreated:0}));
      await page.close();
    }
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
