// Prepare the image before a click so native sharing retains user activation.
const qrShareButton=document.getElementById('qr-share');
if(qrShareButton){
  const status=document.getElementById('qr-share-status');
  const copyField=document.getElementById('qr-share-link');
  const shareUrl=new URL(qrShareButton.dataset.shareUrl,location.href).href;
  let qrFile=null;
  async function prepareQrImage(){
    if(typeof navigator.share!=='function'||typeof navigator.canShare!=='function')return;
    const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),10000);
    try{
      const imageUrl=new URL(shareUrl); imageUrl.pathname+='/png';
      const response=await fetch(imageUrl.href,{signal:controller.signal});
      if(!response.ok)throw new Error('Image unavailable');
      const blob=await response.blob();
      if(blob.type!=='image/png'||!blob.size)throw new Error('Invalid image');
      qrFile=new File([blob],'free-beginners-workshop-qr.png',{type:'image/png'});
    }catch{qrFile=null}
    finally{clearTimeout(timeout)}
  }
  async function copyQrCardLink(){
    try{await navigator.clipboard.writeText(shareUrl);status.textContent='QR card link copied.'}
    catch{copyField.value=shareUrl;copyField.hidden=false;copyField.focus();copyField.select();status.textContent='Copy this link to share the QR card.'}
  }
  prepareQrImage().finally(()=>{qrShareButton.disabled=false});
  qrShareButton.addEventListener('click',async()=>{
    status.textContent='';copyField.hidden=true;qrShareButton.disabled=true;
    try{
      if(typeof navigator.share==='function'){
        const data={url:shareUrl,title:'Free Beginners Go Workshop'};
        if(qrFile&&typeof navigator.canShare==='function'&&navigator.canShare({files:[qrFile]})){
          data.files=[qrFile];
          if(!navigator.canShare(data))delete data.url;
        }
        await navigator.share(data);
        if(!data.files)status.textContent='Card link shared. To share the image too, use Download and attach it in your app.';
      }
      else await copyQrCardLink();
    }catch(error){if(error.name!=='AbortError')await copyQrCardLink()}
    finally{qrShareButton.disabled=false}
  });
}
