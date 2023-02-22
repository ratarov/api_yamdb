import csv
from pathlib import Path

from django.core.management.base import BaseCommand

from reviews.models import Category, Comment, Genre, Review, Title, User


class Command(BaseCommand):
    help = 'Fills the database with data from csv-file in static folder'

    def handle(self, *args, **kwargs):
        FILE_HANDLE = (
            ('category.csv', Category, {}),
            ('genre.csv', Genre, {}),
            ('users.csv', User, {}),
            ('titles.csv', Title, {'category': 'category_id'}),
            ('genre_title.csv', Title.genre.through, {}),
            ('review.csv', Review, {'author': 'author_id'}),
            ('comments.csv', Comment, {'author': 'author_id'}),
        )
        for file, model, replace in FILE_HANDLE:
            self.stdout.write(f'{"---"*40}\nОткрываем файл {file}')
            file_path = Path('static', 'data', file)
            if not file_path.exists():
                self.stderr.write(f'Файл {file} не найден')
                continue
            with open(file_path, mode='r', encoding='utf8') as f:
                self.stdout.write(f'Начинаем импорт из файла {file}')
                reader = csv.DictReader(f)
                counter = 0
                objects_to_create = []
                for row in reader:
                    counter += 1
                    args = dict(**row)
                    if replace:
                        for old, new in replace.items():
                            args[new] = args.pop(old)
                    try:
                        objects_to_create.append(model(**args))
                    except TypeError:
                        self.stderr.write('Неверный заголовок в csv-файле')
                        break
                try:
                    model.objects.bulk_create(objects_to_create,
                                              ignore_conflicts=True)
                    self.stdout.write(
                        f'Добавлено объектов: {len(objects_to_create)}; '
                        f'строк в документе: {counter}')
                except ValueError:
                    self.stderr.write('Ошибка заполнения csv. Импорт отменен')
