#!/usr/bin/env node

import puppeteer from 'puppeteer'
import fs from 'node:fs'
import { promisify } from 'node:util';
import child_process from 'node:child_process';
const exec = promisify(child_process.exec);

import sharp from "sharp";

const CHROME_ARGS = [
    '--disable-sync',
    '--no-pings',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-default-apps',
    '--ash-no-nudges',
    '--disable-infobars',
    '--disable-blink-features=AutomationControlled',
    '--js-flags="--random-seed=1157259159"',
    '--deterministic-mode',
    '--deterministic-fetch',
    '--start-maximized',
    '--test-type=gpu',
    '--disable-search-engine-choice-screen',
    '--disable-session-crashed-bubble',
    '--hide-crash-restore-bubble',
    '--suppress-message-center-popups',
    '--disable-client-side-phishing-detection',
    '--disable-domain-reliability',
    '--disable-component-update',
    '--disable-datasaver-prompt',
    '--disable-hang-monitor',
    '--disable-speech-synthesis-api',
    '--disable-speech-api',
    '--disable-print-preview',
    '--safebrowsing-disable-auto-update',
    '--deny-permission-prompts',
    '--disable-external-intent-requests',
    '--disable-notifications',
    '--disable-desktop-notifications',
    '--noerrdialogs',
    '--disable-popup-blocking',
    '--disable-prompt-on-repost',
    '--silent-debugger-extension-api',
    '--block-new-web-contents',
    '--metrics-recording-only',
    '--disable-breakpad',
    '--run-all-compositor-stages-before-draw',
    '--use-fake-device-for-media-stream',
    '--simulate-outdated-no-au=Tue, 31 Dec 2099 23:59:59 GMT',
    '--force-gpu-mem-available-mb=4096',
    '--password-store=basic',
    '--use-mock-keychain',
    '--disable-cookie-encryption',
    '--allow-legacy-extension-manifests',
    '--disable-gesture-requirement-for-media-playback',
    '--font-render-hinting=none',
    '--force-color-profile=srgb',
    '--disable-partial-raster',
    '--disable-skia-runtime-opts',
    '--disable-2d-canvas-clip-aa',
    '--disable-lazy-loading',
    '--disable-renderer-backgrounding',
    '--disable-background-networking',
    '--disable-background-timer-throttling',
    '--disable-backgrounding-occluded-windows',
    '--disable-ipc-flooding-protection',
    '--disable-extensions-http-throttling',
    '--disable-field-trial-config',
    '--disable-back-forward-cache',
    '--window-size=1440,2000',
    '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 ArchiveBox/0.8.6rc2 (+https://github.com/ArchiveBox/ArchiveBox/)',
    // '--user-data-dir=/Volumes/NVME/Users/squash/Local/Code/archiveboxes/ArchiveBox7/data3/lib/arm64-darwin/npm/out2/personas/Default/chrome_profile',
    // '--profile-directory=Default',
]


// Max texture size of the software GL backand of chronium. (16384px or 4096px)
// https://issues.chromium.org/issues/41347676
export const MAX_SIZE_PX = 16384;
const JPG_QUALITY = 85;

async function scrollDown(page, {timeout=120_000, scroll_delay=1350, scroll_distance=980, scroll_limit=10}={}) {
    const starting_height = await page.evaluate('document.body.scrollHeight');
    let last_height = starting_height

    let scroll_count = 0;
    let scroll_position = scroll_count * scroll_distance
    // await page.bringToFront()

    // scroll to top
    await page.evaluate(() => { window.scrollTo({ top: 0, left: 0, behavior: 'smooth' }); });

    while ((scroll_count < scroll_limit) && ((scroll_delay * scroll_count) < timeout)) {
        console.log(`[⬇️] Scrolling down ${scroll_count}x 1000px... (${scroll_position}/${last_height})`)
        await page.evaluate((y_offset) => { window.scrollTo({ top: y_offset, left: 0, behavior: 'smooth' }); }, scroll_position);
        scroll_count++
        scroll_position = scroll_count * scroll_distance

        // check if any new content was added / if we are infiniscrolling
        let new_height = await page.evaluate('document.body.scrollHeight')
        const added_px = new_height - last_height
        if (added_px > 0) {
            console.log('[✚] Detected infini-scrolling...', `${last_height}+${added_px} => ${new_height}`)
        } else if (scroll_position >= new_height + scroll_distance) {
            // we've reached the bottom, condition isn't true until we've tried to go n+1 past the end (which is fine)
            if (scroll_count > 2)
                break
        }
        last_height = new_height
        
        // sleep 2s, perform the smooth scroll down by 1000px, and increment the counter
        await wait(scroll_delay);

        // facebook watch pages infiniscroll (more and more recommendations forever), stop them after 3 pages
        // if (page.url().startsWith('https://www.facebook.com/watch/?v') && scroll_count > 3) break
    }

    // scroll to bottom
    if (scroll_position < last_height) {
        await page.evaluate(() => { window.scrollTo({ top: document.body.scrollHeight, left: 0, behavior: 'smooth' }); });
        await wait(scroll_delay)
        await page.evaluate(() => { window.scrollTo({ top: document.body.scrollHeight, left: 0, behavior: 'smooth' }); });
    }

    // Always wait an additional 2sec at the end for scroll animations / loading / rendering to settle down
    console.log('[📉] Reached bottom of the page.', `(${scroll_position}/${last_height})`)
    await wait(scroll_delay);
    await page.evaluate(() => { window.scrollTo({ top: 0, left: 0, behavior: 'smooth' }); });
    await wait(scroll_delay);

    return last_height
}


const captureLargeScreenshot = async (page) => {
  const scroll_height = await page.evaluate(() => document.body.scrollHeight)
  await page.setViewport({width: 1440, height: scroll_height, deviceScaleFactor: 2})
  await page.evaluate(() => {window.scrollTo(0, document.body.scrollHeight)})

  const viewport = page.viewport();
  const deviceScaleFactor = viewport.deviceScaleFactor ?? 1;
  const width = viewport.width;

  const pageHeight = await getPageHeight(page);
  const screenshots = [];

  let currentYPosition = 0;

  const scaledPageHeight = pageHeight * deviceScaleFactor;

  while (currentYPosition < scaledPageHeight) {
    const clipHeight = Math.min(4000, pageHeight)

    console.log(`    * scrolling ${currentYPosition/deviceScaleFactor}..${(currentYPosition + clipHeight)/deviceScaleFactor}px...`)
    const screenshotPromise = page.screenshot({
      clip: {
        x: 0,
        y: currentYPosition,
        width: width,
        height: clipHeight,
      },
      omitBackground: false,
      type: 'png',
    });
    await wait(250)
    const value = await screenshotPromise
    screenshots.push(value)
    currentYPosition += clipHeight;
  }
//   const values = await Promise.all(screenshotPromises);
//   screenshots.push(...values);

  console.log(`    * stitching ${screenshots.length} screenshots together...`)
  try {
    const screenshotBuffer = await stitchImages(
      width * deviceScaleFactor,
      scaledPageHeight,
      screenshots,
    );

    const uint8Array = new Uint8Array(screenshotBuffer);

    return uint8Array;
  } catch (err) {
    console.error("Error stitching screenshots:", err);
    throw err; // Propagate the error
  }
};

const stitchImages = async (
  w,
  h,
  screenshots
) => {
  let currentHeight = 0;
  const compositeOperations = [];

  for (let i = 0; i < screenshots.length; i++) {
    const screenshot = screenshots[i];

    try {
      const img = sharp(screenshot);
      const { height: imgHeight } = await img.metadata();

      // Convert Uint8Array to Buffer
      const bufferInput = Buffer.from(screenshot);

      // Collect composite operations
      compositeOperations.push({
        input: bufferInput,
        top: currentHeight,
        left: 0,
      });

      currentHeight += imgHeight ?? 0;
    } catch (err) {
      console.error(`Error processing screenshot ${i}:`, err);
      throw err;
    }
  }

  const img = sharp({
    create: {
      width: w,
      height: h,
      channels: 4,
      background: { r: 255, g: 255, b: 255, alpha: 0 },
    },
    limitInputPixels: h * w,
  });

  const result = img.composite(compositeOperations);

  return await result.jpeg({quality: JPG_QUALITY}).toBuffer();
};

const getPageHeight = async (page) => {
  return await page.evaluate(() => document.documentElement.scrollHeight);
};
const wait = (ms) => new Promise(res => {
    setTimeout(res, ms)
})

async function main(url) {
    // const {stdout, stderr} = await exec('//Users/squash/Local/Code/archiveboxes/ArchiveBox7/archivebox/pkgs/abx-plugin-chrome/abx_plugin_chrome/config.py CHROME_ARGS')
    // const launch_args = stdout.trim().split('\n')
    // console.log(launch_args)
    console.log('[+] Opening browser...')
    const browser = await puppeteer.launch({
        headless: true,
        // waitForInitialPage: true,
        args: CHROME_ARGS,
        // ignoreDefaultArgs: true,
    })
    const page = await browser.newPage()
    await page.setViewport({width: 1440, height: 1080, deviceScaleFactor: 2})
    console.log('[+] Loading page (up to 30s)...', url)
    await page.goto(url, {waitUntil: 'networkidle2', timeout: 30_000})
    const cdp = await page.target().createCDPSession();
    // console.log('[+] Waiting 2.5s for final animations and loading...')
    // await wait(2_500)    // plenty of time for loading later during scrolling

    // save the page title
    const title = await page.title()
    console.log('    > ./title.txt         tags: text,title,text/plain')
    fs.writeFileSync('title.txt', title)
    
    // screnshot the top 1440x1080 pixels
    console.log('[+] Taking screenshots...')
    await page.evaluate(() => {window.scrollTo(0, 0)})
    await wait(150)
    await page.screenshot({path: 'top.jpg', type: 'jpeg', quality: 90, clip: {x: 0, y: 0, width: 1440, height: 1080}})  // 4:3 ratio
    console.log('    > ./thumbnail.jpg     tags: image,screenshot,thumbnail,mimetype=image/jpeg,ext=jpg,ratio=4:3,dimensions=1440x1080')
    
    // screenshot the entire page
    // const scroll_height = await page.evaluate(() => document.body.scrollHeight)
    await scrollDown(page)
    const screenshot = await captureLargeScreenshot(page)
    await fs.writeFileSync('screenshot.jpg', screenshot)
    console.log('    > ./screenshot.jpg    tags: image,screenshot,thumbnail,mimetype=image/jpeg,ext=jpg,dimensions=1440')

    // pdf the entire page
    await page.pdf({path: 'pdf.pdf', format: 'a4', printBackground: true, margin: {top: 0, right: 0, bottom: 0, left: 0}})
    console.log('    > ./pdf.pdf           tags: pdf,document,mimetype=application/pdf,ext=pdf,format=a4')

    // save the DOM as mhtml
    const { data } = await cdp.send('Page.captureSnapshot', { format: 'mhtml' });
    fs.writeFileSync('page.mhtml', data);
    console.log('    > ./page.mhtml        tags: html,mimetype=text/mhtml,ext=mhtml')

    // save the raw DOM as html
    const html = await page.content()
    fs.writeFileSync('dom.html', html)
    console.log('    > ./dom.html          tags: html,text/html')

    // save the inner text
    const text = await page.evaluate(() => document.body.innerText)
    fs.writeFileSync('text.txt', text)
    console.log('    > ./text.txt          tags: text,ext=txt,mimetype=text/plain')

    await browser.close()
}

main(process.argv[2])

