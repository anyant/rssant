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
