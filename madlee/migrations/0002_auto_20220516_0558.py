# Generated by Django 3.2.5 on 2022-05-16 05:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('madlee', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='alert',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
        migrations.AlterField(
            model_name='tag',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
    ]
