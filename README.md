# abx-dl [COMING SOON]


A CLI tool to download *everything* from a given URL (similar to youtube-dl/yt-dlp, forum-dl, gallery-dl, etc.).

Uses headless chrome to get HTML, JS, CSS, images/video/audio/subs, pdf, screenshot, article text, gits src, [and more](https://github.com/ArchiveBox/ArchiveBox#output-formats)...

## Coming Soon

```bash
pip install abx-dl[all]
abx-dl --version
```
```bash
mkdir ~/Downloads/example.com && cd ~/Downloads/example.com
```
```bash
# download everything
abx-dl 'https://example.com'
```
```bash
# download just title + screenshot
abx-dl --extract=title,screenshot 'https://example.com'
ls ./
# index.json  title.txt  screenshot.png
```
```bash
# download a few more things
abx-dl --extract=title,favicon,screenshot,singlefile,media 'https://example.com'
ls ./
# index.json  index.html  title.txt  favicon.ico  screenshot.png  singlefile.html  media/Some_video.mp4
```


#### All Outputs

- `index.json`, `index.html`
- `title.txt`, `title.json`, `headers.json`, `favicon.ico`
- `example.com/*.{html,css,js,png...}`, `warc/`  (saved with `wget-lua`)
- `screenshot.png`, `dom.html`, `output.pdf` (rendered with `chrome`)
- `media/someVideo.mp4`, `media/subtitles`, ... (downloaded with `yt-dlp`)
- `readability/`, `mercury/`, `htmltotext.txt` (article text/markdown)
- `git/` (source code)
- ... [and more](https://github.com/ArchiveBox/ArchiveBox#output-formats) ...

