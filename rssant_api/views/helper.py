from rest_framework.exceptions import PermissionDenied


def check_unionid(user, unionid_s):
    if not unionid_s:
        return
    if not isinstance(unionid_s, list):
        unionid_s = [unionid_s]
    for unionid in unionid_s:
        if unionid.user_id != user.id:
            raise PermissionDenied()
