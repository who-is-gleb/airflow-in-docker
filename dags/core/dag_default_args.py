import datetime

DEFAULT_OWNER = 'gleb.borovoy'


def get_default_arguments(
        owner: str = DEFAULT_OWNER,
        depends_on_past: bool = False,
        email_on_failure: bool = True,
        email_on_retry: bool = False,
        retries: int = 3,
        retry_delay: datetime.timedelta = datetime.timedelta(minutes=5),
        **kwargs):

    res = {
        "owner": owner,
        "depends_on_past": depends_on_past,
        "email_on_failure": email_on_failure,
        "email_on_retry": email_on_retry,
        "retries": retries,
        "retry_delay": retry_delay,
    }
    res.update(kwargs)

    return res
