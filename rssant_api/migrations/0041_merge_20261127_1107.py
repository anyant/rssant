from django.db import migrations


class Migration(migrations.Migration):
    """合并 0040 的两个叶子迁移，修复迁移图冲突。

    两个 0040 都是 2026-11-26 各自生成、都依赖 0039；同时存在时
    manage.py migrate / makemigrations 会报 multiple leaf nodes。
    本迁移没有数据库操作，只用于把迁移图收成单叶子。
    """

    dependencies = [
        ('rssant_api', '0040_feedcreation_error_message'),
        ('rssant_api', '0040_userstory_is_read'),
    ]

    operations = []

