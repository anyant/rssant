import secrets

from rest_framework.permissions import BasePermission

from rssant_config import CONFIG


class AllowServiceClient(BasePermission):
    def has_permission(self, request, view):
        secret = request.META.get('HTTP_X_RSSANT_SERVICE_SECRET') or ''
        expected_secret = CONFIG.service_secret or ''
        return secrets.compare_digest(secret, expected_secret)

    def has_object_permission(self, request, view, obj):
        return self.has_permission(request, view)
