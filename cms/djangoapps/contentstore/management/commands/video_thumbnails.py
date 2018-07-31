"""
Command to add thumbnails to videos.
"""

import logging
from django.core.management import BaseCommand
from edxval.api import get_course_video_ids_with_youtube_profile
from openedx.core.djangoapps.video_config.models import VideoThumbnailSetting, UpdatedCourseVideos
from cms.djangoapps.contentstore.tasks import enqueue_update_thumbnail_tasks

log = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Example usage:
        $ ./manage.py cms video_thumbnails --from-settings
    """
    help = 'Adds thumbnails from YouTube to videos'

    def add_arguments(self, parser):
        """
        Add arguments to the command parser.
        """
        parser.add_argument(
            '--from-settings', '--from_settings',
            dest='from_settings',
            help='Update videos with settings set via django admin',
            action='store_true',
            default=False,
        )

    def _get_command_options(self):
        """
        Returns the command arguments configured via django admin.
        """
        command_settings = self._latest_settings()
        if command_settings.all_course_videos:

            all_course_video_ids = get_course_video_ids_with_youtube_profile()
            updated_course_videos = UpdatedCourseVideos.objects.all().values_list('course_id', 'edx_video_id')
            non_updated_course_videos = [
                course_video_id
                for course_video_id in all_course_video_ids
                if (course_video_id[0], course_video_id[1]) not in list(updated_course_videos)
            ]
            # Video batch to be updated
            course_video_batch = non_updated_course_videos[:command_settings.batch_size]

            log.info(
                ('[Video Thumbnails] Videos(total): %s, '
                 'Videos(updated): %s, Videos(non-updated): %s, '
                 'Videos(updation-in-process): %s'),
                len(all_course_video_ids),
                len(updated_course_videos),
                len(non_updated_course_videos),
                len(course_video_batch),
            )
        else:
            course_video_batch = get_course_video_ids_with_youtube_profile(command_settings.course_ids.split())
            force_update = command_settings.force_update
            commit = command_settings.commit

        return course_video_batch, force_update, commit

    def _latest_settings(self):
        """
        Return the latest version of the VideoThumbnailSetting
        """
        return VideoThumbnailSetting.current()

    def handle(self, *args, **options):
        """
        Invokes the video thumbnail enqueue function.
        """
        command_settings = self._latest_settings()
        course_video_batch, force_update, commit = self._get_command_options()
        command_run = command_settings.increment_run() if commit else -1
        enqueue_update_thumbnail_tasks(
            course_video_ids=course_video_batch, commit=commit, command_run=command_run, force_update=force_update
        )

        if commit and options.get('from_settings') and command_settings.all_videos:
            UpdatedCourseVideos.objects.bulk_create([
                UpdatedCourseVideos(course_id=course_video_id[0],
                                    edx_video_id=course_video_id[1],
                                    command_run=command_run)
                for course_video_id in course_video_batch
            ])
