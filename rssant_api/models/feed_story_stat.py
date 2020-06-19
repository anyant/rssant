from .helper import Model, models, optional


class FeedStoryStat(Model):
    """
    feed_id -> story stats
    """

    id = models.PositiveIntegerField(primary_key=True, help_text='feed id')

    # TODO: will migrate from feed table
    monthly_story_count_data = models.BinaryField(
        **optional, max_length=514, help_text="monthly story count data")

    # TODO: will migrate from feed table
    checksum_data = models.BinaryField(
        **optional, max_length=4096, help_text="feed checksum data")

    unique_ids_data = models.BinaryField(
        **optional, max_length=100 * 1024, help_text="unique ids data")

    @classmethod
    def _create_if_not_exists(cls, feed_id: int) -> bool:
        is_exists = FeedStoryStat.objects.filter(pk=feed_id).exists()
        if not is_exists:
            FeedStoryStat(id=feed_id).save()

    @classmethod
    def _create_or_update(cls, feed_id, **kwargs):
        cls._create_if_not_exists(feed_id)
        updated = FeedStoryStat.objects.filter(pk=feed_id)\
            .update(**kwargs)
        if updated <= 0:
            raise ValueError(f'update FeedStoryStat#{feed_id} failed')

    @classmethod
    def save_unique_ids_data(cls, feed_id: int, unique_ids_data: bytes):
        cls._create_or_update(feed_id, unique_ids_data=unique_ids_data)

    @classmethod
    def save_monthly_story_count_data(cls, feed_id: int, monthly_story_count_data: bytes):
        cls._create_or_update(feed_id, monthly_story_count_data=monthly_story_count_data)

    @classmethod
    def save_checksum_data(cls, feed_id: int, checksum_data: bytes):
        cls._create_or_update(feed_id, checksum_data=checksum_data)
