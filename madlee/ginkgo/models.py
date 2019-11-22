from django.db import models

from ..models import TimedModel, JSONField, User

class Leaf(TimedModel):
    name = models.CharField(max_length=50, unique=True)
    slot = models.CharField(max_length=8)
    size = models.IntegerField()


    def __str__(self):
        return self.name




class Block(TimedModel):
    pass
