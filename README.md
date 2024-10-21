# abx-dl [COMING SOON]

## Coming Soon

```bash
pip install abx-dl[all]
abx-dl version

mkdir ~/Downloads/example.com && cd ~/Downloads/example.com
abx-dl 'https://example.com'
ls ./
# index.json  index.html  title.txt  title.json headers.json  favicon.ico
# example.com/index.html  example.com/main.js  example.com/css/main.css warc/  # saved with wget-at
# dom.html output.pdf screenshot.png    # rendered with headless chrome
# readability/ mercury/ htmltotext.txt  # parsed article text outputs

abx-dl --extract=title,favicon,screenshot,singlefile,screenshot,media 'https://example.com'
# index.json  index.html  title.txt  favicon.ico  singlefile.html  screenshot.png  media/Some_video.mp4
```

A CLI tool to download *everything* from a given URL (similar to youtube-dl/yt-dlp, forum-dl, gallery-dl, etc.). Uses headless chrome to get HTML, JS, CSS, images/video/audio/subs, pdf, screenshot, article text, gits src, and more...
