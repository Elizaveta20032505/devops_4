# Лабораторная работа №4 — Kafka + Vault + CI/CD

Продолжение лабы №3: после `/predict` результат публикуется в **Apache Kafka** (producer); тот же сервис поднимает **consumer** и кладёт сообщения во внутреннюю очередь. Параметры подключения и имя топика читаются из **Vault** (`secret/kafka`; для образа `apache/kafka` внутри сети compose используется `bootstrap_servers=kafka:19092`), креды БД по-прежнему только из `secret/postgres`.

Сервисы: `vault`, `db`, `kafka`, `api`. CI (`Jenkinsfile`): build/push образа `devops4-api`, триггер CD. CD (`CD/Jenkinsfile`): compose, сиды, `scripts/run_scenarios.py`.
