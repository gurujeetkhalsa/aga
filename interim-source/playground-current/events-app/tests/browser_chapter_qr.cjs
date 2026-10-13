const fs=require('fs'),path=require('path'),assert=require('assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
const base='https://workshop.test/events/learn-go-in-an-hour';
const chapters=[{chapter_id:22385,chapter_code:'NGOC',chapter_name:'National Go Center'},{chapter_id:123,chapter_code:'TEST',chapter_name:'Test Go Club'}];
const png=fs.readFileSync(path.join(root,'test-results/chapter-ngoc-qr.png'));
const selector=fs.readFileSync(path.join(root,'workshop_chapter_qr.html'),'utf8');
const script=fs.readFileSync(path.join(root,'workshop_qr_share.js'),'utf8');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'chrome'});
 try{
  for(const width of [390,1280]){
   const page=await browser.newPage({viewport:{width,height:844}}),errors=[];
   page.on('pageerror',e=>errors.push(e.message));
   let fail=true;
   await page.addInitScript(()=>{
    Object.defineProperty(navigator,'canShare',{configurable:true,value:data=>Boolean(data.files?.length)});
    Object.defineProperty(navigator,'share',{configurable:true,value:async data=>{window.shared=data}});
   });
   await page.route('https://workshop.test/**',route=>{
    const url=new URL(route.request().url());
    if(url.pathname==='/api/workshop-chapters'){
     if(fail){fail=false;return route.fulfill({status:503,json:{ok:false}})}
     return route.fulfill({json:{ok:true,chapters}});
    }
    if(url.pathname.endsWith('/chapter-qr'))return route.fulfill({contentType:'text/html',body:selector});
    if(url.pathname.endsWith('/png')){
     assert.equal(url.searchParams.get('ref'),'22385');
     return route.fulfill({contentType:'image/png',body:png});
    }
    if(url.pathname.endsWith('/svg'))return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg"/>'});
    if(url.pathname.endsWith('.js'))return route.fulfill({contentType:'text/javascript',body:script});
    if(url.pathname.endsWith('/workshop-qr')){
     assert.equal(url.searchParams.get('ref'),'22385');
     const card=fs.readFileSync(path.join(root,'workshop_qr_page.html'),'utf8')
      .replaceAll('__REGISTRATION_URL__',base+'?ref=22385').replaceAll('__CARD_URL__',base+'/workshop-qr?ref=22385')
      .replaceAll('__PNG_URL__',base+'/workshop-qr/png?ref=22385').replaceAll('__CHAPTER_LABEL__','National Go Center (NGOC)');
     return route.fulfill({contentType:'text/html',body:card});
    }
    throw new Error('Unexpected request: '+url);
   });
   await page.goto(base+'/chapter-qr');
   await page.getByRole('button',{name:'Try again'}).click();
   const search=page.getByLabel('Chapter name or short code',{exact:true});
   await search.fill('NGOC');
   await page.locator('#qr-preview').evaluate(img=>img.decode());
   assert.equal(await page.locator('#chapter-name').textContent(),'National Go Center (NGOC)');
   const d=page.waitForEvent('download');await page.getByRole('link',{name:'Download QR image',exact:true}).click();
   assert.equal((await d).suggestedFilename(),'workshop-NGOC-qr.png');
   await search.fill('National Go Center');
   assert(await page.locator('#selection').isVisible());
   await search.fill('Not a chapter');assert(await page.locator('#selection').isHidden());
   await search.fill('');assert(await page.locator('#selection').isHidden());
   await search.fill('National');
   await search.press('ArrowDown');await search.press('Enter');
   // Browser datalist behavior differs by platform; the exact selected label works everywhere.
   await search.fill('National Go Center (NGOC)');
   await page.locator('#qr-preview').evaluate(img=>img.decode());
   assert.equal(await page.locator('#download-svg').getAttribute('href'),'/events/learn-go-in-an-hour/workshop-qr/svg?ref=22385');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
   await page.screenshot({path:path.join(root,`test-results/chapter-selector-${width}.png`),fullPage:true});
   assert.equal(await page.getByRole('link',{name:'Open shareable QR card'}).count(),0);
   await page.goto(base+'/workshop-qr?ref=22385');
   await page.getByRole('button',{name:'Share QR card'}).click();
   assert.equal(await page.evaluate(()=>window.shared.url),base+'/workshop-qr?ref=22385');
   assert.equal(await page.evaluate(()=>window.shared.files[0].type),'image/png');
   assert.equal(await page.getByRole('link',{name:'See workshops & register',exact:true}).getAttribute('href'),base+'?ref=22385');
   const sharedBytes=await page.evaluate(async()=>Array.from(new Uint8Array(await window.shared.files[0].arrayBuffer())));
   assert.deepEqual(Buffer.from(sharedBytes),png);
   assert.deepEqual(errors,[]);
   await page.close();console.log(`Chapter selector ${width}px: lookup, retry, PNG/SVG links, clearing selection and chapter-preserving share passed.`);
  }
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
