# Установка Docker
sudo apt install -y docker.io

# Добавить Docker в автозапуск и запустить
sudo systemctl enable docker
sudo systemctl start docker

# Установка Docker Compose
sudo mkdir -p /usr/lib/docker/cli-plugins
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" -o /usr/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/lib/docker/cli-plugins/docker-compose


# клонируем проект , переходим в него
# не забываем создать .env файл и credentials.json

# запускаем
docker-compose up --build -d

# если надо остановить
docker-compose down

# после git pull
docker-compose down
docker-compose up --build -d

# при изменениях в tasks/ parsers/ tasks.py funcs_db.py celery_config.py logging_config.py logg_set.py
# автоматически на сервере celery_worker перезапускается

# при изменениях в django_app/ logg_set.py
# автоматически на сервере django_app перезапускается

# чтобы мониторить таски с локального компа надо прокинуть ssh порт
# ssh -L 5555:localhost:5555 root@95.215.56.26  
# http://localhost:5555/

# Подключиться к БД 
# docker exec -it postgres psql -U romandikarev rencon


# Время в локах смотрим в message 
# При установке времени в тасках - устанавливается время по МСК

# Удачи!