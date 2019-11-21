from django.db import models

from ..models import TimedModel, JSONField, User


class DataSouce(TimedModel):
    KIND_CHOICES = (('DB', 'Database'), ('MSGP', 'Message Pack'))
    name = models.CharField(max_length=100, db_index=True)
    author = models.ForeignKey(User, on_delete=models.PROTECT)
    kind = models.CharField(max_length=4, choices=KIND_CHOICES)
    config = JSONField()


class Plot(TimedModel):
    name = models.CharField(max_length=100, db_index=True)
    author = models.ForeignKey(User, on_delete=models.PROTECT)
    config = JSONField()

