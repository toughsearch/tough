from tough.dt import get_date

row = b'46.229.168.140 - - [04/Apr/2019:23:03:24 +0000] "GET /login?consumer=portal-ru&locale=ru_RU&return_to=https%3A%2F%2Fespritgames.ru%2Fforum%2Ftopic%2F%25D0%25BD%25D0%25B5%25D0%25BB%25D1%258C%25D0%25B7%25D1%258F-%25D0%25B2%25D1%258B%25D0%25B1%25D1%2580%25D0%25B0%25D1%2582%25D1%258C%25D0%25BF%25D0%25B0%25D1%2580%25D1%2582%25D0%25BD%25D0%25B5%25D1%2580%25D0%25B0-%25D0%25B4%25D0%25BB%25D1%258F-%25D1%2581%25D0%25B2%25D0%25B0%25D0%25B4%25D1%258C%25D0%25B1%25D1%258B%2F&theme HTTP/1.1" 200 5298 "-" "Mozilla/5.0 (compatible; SemrushBot/3~bl; +http://www.semrush.com/bot.html)"'
print(get_date(row, br'\[(.*)\]', b'%d/%b/%Y:%H:%M:%S %z'))
