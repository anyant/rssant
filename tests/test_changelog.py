from rssant_common.changelog import ChangeLog, ChangeLogList


def test_changelog():
    changelog = ChangeLog.from_path('docs/changelog/1.0.0.md')
    assert changelog.version == '1.0.0'
    assert changelog.date
    assert changelog.title
    assert changelog.html


def test_changelog_list():
    changelog_list = ChangeLogList(
        title='蚁阅更新日志',
        link='https://rss.anyant.com/changelog',
        directory='docs/changelog',
    )
    assert changelog_list.items
    assert changelog_list.to_atom()
    assert changelog_list.to_html()
