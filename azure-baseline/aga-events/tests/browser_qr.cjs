// Render the vector download so it can be visually inspected and independently scanned.
const fs=require('fs');
const path=require('path');
const {chromium}=require('playwright');
(async()=>{
 const root=path.resolve(__dirname,'..');
 const browser=await chromium.launch({headless:true,channel:'chrome'});
 try{
  const page=await browser.newPage({viewport:{width:1200,height:1200}});
  await page.setContent('<style>body{margin:0}</style>'+fs.readFileSync(path.join(root,'downloads/free-beginners-workshop-qr.svg'),'utf8'));
  await page.screenshot({path:path.join(root,'test-results/workshop-qr-svg-render.png')});
  console.log('SVG artwork rendered for scan verification.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
