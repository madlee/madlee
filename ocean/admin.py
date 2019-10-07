from django.contrib import admin

from .models import Tag, Alert




@admin.register(Tag)
class TagAdmin(admin.ModelAdmin):
    list_display = ['name', 'created_at']
    readonly_fields = 'created_at', 'updated_at'
    date_hierarchy = 'created_at'


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    pass
