from authors import authors_obj
from sons import sons_obj

if __name__ == "__main__":
    # 爬取诗人, 存入mongodb
    authors_obj.go()

    # 跟据所获诗人 爬取诗歌 存入mongodb
    sons_obj.go()