from django.contrib import admin

from .models import Leaf, Block


@admin.register(Leaf)
class LeafAdmin(admin.ModelAdmin):
    list_display = 'name', 'key', 'slot', 'size', 'created_at', 'updated_at'
    readonly_fields = 'created_at', 'updated_at'
    date_hierarchy = 'updated_at'
    list_filter = 'name', 'key', 'slot', 'size'
    search_fields = 'name', 'key',



@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    list_display = 'leaf', 'slot', 'size', 'start', 'finish'
    readonly_fields = 'created_at', 'updated_at'
    date_hierarchy = 'start'
    list_filter = 'leaf', 
    search_fields = 'leaf__name', 'leaf__key'

