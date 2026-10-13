// Public QR-card checks. Every request and share target is local/fake.
const fs=require('fs'),path=require('path'),assert=require('assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
const base='https://workshop.test/events/learn-go-in-an-hour',cardUrl=base+'/workshop-qr';
const html=fs.readFileSync(path.join(root,'workshop_qr_page.html'),'utf8').replaceAll('__REGISTRATION_URL__',base).replaceAll('__CARD_URL__',cardUrl).replaceAll('__PNG_URL__',base+'/workshop-qr/png').replaceAll('__CHAPTER_LABEL__','');
const script=fs.readFileSync(path.join(root,'workshop_qr_share.js'),'utf8');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'chrome'});
 try{
  for(const width of [320,390,1280]){
   const page=await browser.newPage({viewport:{width,height:760}}),errors=[];
   page.on('pageerror',e=>errors.push(e.message));
   await page.addInitScript(()=>{
    Object.defineProperty(navigator,'canShare',{configurable:true,value:data=>Boolean(data.files?.length)});
    Object.defineProperty(navigator,'share',{configurable:true,value:async data=>{
     window.shared=data;window.shareWasActive=navigator.userActivation.isActive;
    }});
   });
   await page.route('https://workshop.test/**',route=>{
    const url=route.request().url();
    if(url.endsWith('.js'))return route.fulfill({contentType:'text/javascript',body:script});
    if(url.endsWith('/png'))return route.fulfill({contentType:'image/png',headers:{'Content-Disposition':'attachment; filename="free-beginners-workshop-qr.png"'},body:fs.readFileSync(path.join(root,'test-results/workshop-phone-qr.png'))});
    assert.equal(url,cardUrl);return route.fulfill({contentType:'text/html',body:html});
   });
   await page.goto(cardUrl);
   await page.locator('img').evaluate(image=>image.decode());
   assert.equal(await page.getByRole('link',{name:'See workshops & register',exact:true}).getAttribute('href'),base);
   assert.equal(await page.getByRole('link',{name:'See upcoming workshops and register',exact:true}).getAttribute('href'),base);
   const download=page.getByRole('link',{name:'Download PNG'}),share=page.getByRole('button',{name:'Share QR card'});
   const a=await download.boundingBox(),b=await share.boundingBox();
   assert(a.x<b.x&&Math.abs(a.y-b.y)<1&&a.width>=44&&b.width>=44);
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
   const d=page.waitForEvent('download');await download.click();assert.equal((await d).suggestedFilename(),'free-beginners-workshop-qr.png');
   await share.click();assert.equal(await page.evaluate(()=>window.shared.url),cardUrl);
   assert.equal(await page.evaluate(()=>window.shareWasActive),true);
   const file=await page.evaluate(async()=>({name:window.shared.files[0].name,type:window.shared.files[0].type,bytes:Array.from(new Uint8Array(await window.shared.files[0].arrayBuffer()))}));
   assert.equal(file.name,'free-beginners-workshop-qr.png');assert.equal(file.type,'image/png');
   assert.deepEqual(Buffer.from(file.bytes),fs.readFileSync(path.join(root,'test-results/workshop-phone-qr.png')));
   await page.evaluate(()=>Object.defineProperty(navigator,'canShare',{configurable:true,value:data=>Boolean(data.files)&&!data.url}));
   await share.click();assert.equal(await page.evaluate(()=>window.shared.files.length),1);assert.equal(await page.evaluate(()=>window.shared.url),undefined);
   await page.evaluate(()=>Object.defineProperty(navigator,'canShare',{configurable:true,value:()=>false}));
   await share.click();assert.equal(await page.evaluate(()=>window.shared.url),cardUrl);assert.equal(await page.evaluate(()=>window.shared.files),undefined);
   await page.evaluate(()=>Object.defineProperty(navigator,'share',{configurable:true,value:async()=>{throw new DOMException('Cancelled','AbortError')}}));
   await share.click();assert.equal(await page.locator('#qr-share-status').textContent(),'');
   await page.evaluate(()=>{Object.defineProperty(navigator,'share',{configurable:true,value:undefined});Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:async text=>window.copied=text}})});
   await share.click();assert.equal(await page.evaluate(()=>window.copied),cardUrl);
   await page.locator('#qr-share-status').evaluate(el=>el.textContent='');
   await page.mouse.move(0,0);
   await page.screenshot({path:path.join(root,`test-results/public-qr-card-${width}.png`),fullPage:true});
   assert.deepEqual(errors,[]);await page.close();
   console.log(`Public QR card ${width}px: exact PNG share with active gesture, image-only and link fallbacks, cancellation, download and responsive layout passed.`);
  }
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
