import typing
import os
import io
import datetime
from pathlib import Path

from packaging.version import parse as parse_version
from markdown import Markdown
from mako.template import Template


class ChangeLog:

    version: str
    date: datetime.date
    title: str
    html: str

    def __init__(self, meta: dict, html: str):
        version = meta.get('version')
        if not version:
            raise ValueError('version is required')
        version = str(parse_version(version))
        date = meta.get('date')
        if not date:
            raise ValueError('date is required')
        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        title = meta.get('title')
        if not title:
            raise ValueError('title is required')
        self.version = version
        self.date = date
        self.title = title
        self.html = html

    def __repr__(self):
        cls_name = type(self).__name__
        date = self.date.strftime('%Y-%m-%d')
        return f'<{cls_name} {self.version} date={date} title={self.title!r}>'

    @classmethod
    def from_text(cls, text: str) -> "ChangeLog":
        extensions = ['extra', 'meta']
        md = Markdown(extensions=extensions)
        html = md.convert(text)
        meta = {}
        for k, v in md.Meta.items():
            v = ' '.join(v).strip('"\' ')
            meta[k] = v
        return cls(meta, html)

    @classmethod
    def from_file(cls, file: io.FileIO) -> "ChangeLog":
        text = file.read()
        return cls.from_text(text)

    @classmethod
    def from_path(cls, filepath: str) -> "ChangeLog":
        with open(filepath) as f:
            return cls.from_file(f)


_res_dir = Path(__file__).parent / 'resources'
atom_template = _res_dir / 'changelog.atom.mako'
html_template = _res_dir / 'changelog.html.mako'
normalize_css = _res_dir / 'normalize.css'
github_markdown_css = _res_dir / 'github-markdown.css'


class ChangeLogList:
    def __init__(
        self,
        items: typing.List[ChangeLog] = None,
        *,
        directory: str = None,
        title: str = None,
        link: str = None,
    ):
        self.items = items or []
        self.title = title or ''
        self.link = (link or '').rstrip('/')
        if directory:
            self.load_directory(directory)

    @staticmethod
    def _format_date(dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _get_dt_updated(self) -> datetime.date:
        if not self.items:
            return None
        return max(x.date for x in self.items)

    def load_directory(self, directory: str) -> None:
        changelogs = []
        with os.scandir(directory) as entry_it:
            for entry in entry_it:
                changelogs.append(ChangeLog.from_path(entry.path))
        changelogs = list(sorted(changelogs, key=lambda x: x.date, reverse=True))
        self.items.extend(changelogs)

    def to_atom(self) -> str:
        template = Template(atom_template.read_text())
        return template.render(
            format_date=self._format_date,
            title=self.title,
            link=self.link,
            updated=self._get_dt_updated(),
            changelogs=self.items,
        )

    def to_html(self) -> str:
        template = Template(html_template.read_text())
        return template.render(
            format_date=self._format_date,
            normalize_css=normalize_css.read_text(),
            github_markdown_css=github_markdown_css.read_text(),
            title=self.title,
            link=self.link,
            updated=self._get_dt_updated(),
            changelogs=self.items,
        )
