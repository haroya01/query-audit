import hashlib
import logging
from pathlib import Path

log = logging.getLogger("mkdocs.hooks.translation_status")

SUFFIX = ".ko.md"


def source_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def on_page_markdown(markdown, page, config, files, **kwargs):
    src = page.file.src_uri
    if not src.endswith(SUFFIX):
        return markdown
    source = Path(config["docs_dir"]) / (src[: -len(SUFFIX)] + ".md")
    english_url = "../" * page.url.count("/") + page.url.split("/", 1)[1]
    recorded = str(page.meta.get("source_digest", ""))
    current = source_digest(source)
    if recorded == current and src == "index" + SUFFIX:
        notice = ""
    elif recorded == current:
        notice = (
            '!!! info "영어 원문의 번역"\n'
            f"    이 페이지는 [영어 원문]({english_url})을 번역했어요. 내용이 다르면 영어 원문이 기준이에요.\n\n"
        )
    else:
        log.info("Korean page %s is older than %s", src, source.name)
        notice = (
            '!!! warning "영어 원문보다 오래된 번역"\n'
            f"    영어 원문이 이 번역 뒤에 바뀌었어요. 최신 내용은 [영어 원문]({english_url})을 보세요.\n\n"
        )
    return notice + markdown
