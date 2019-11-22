from django.db import models

from ..models import TimedModel, JSONField, User

class Leaf(TimedModel):
    name = models.CharField(max_length=50)
    key  = models.CharField(max_length=50)
    slot = models.CharField(max_length=8)
    size = models.IntegerField()

    def __str__(self):
        return '%s|%s' % (self.name, self.key)

    class Meta:
        unique_together = ('name', 'key'),


class Block(TimedModel):
    leaf   = models.ForeignKey(Leaf, on_delete=models.PROTECT)
    slot   = models.IntegerField()
    start  = models.DateTimeField()
    finish = models.DateTimeField()
    size   = models.IntegerField()
    data   = models.BinaryField()

    def __str__(self):
        return '[%s] %d' % (self.leaf, self.slot)

    class Meta:
        unique_together = ('leaf', 'slot'),

