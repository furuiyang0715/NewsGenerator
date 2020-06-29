import pprint

from base import NewsBase


class ChangeTableSelector(NewsBase):
    def start(self):
        self._target_init()

        # sql = 'select Date, Title, Content, CREATETIMEJZ, UPDATETIMEJZ from news_generate_morningtop10;'
        # datas = self.target_client.select_all(sql)
        # for data in datas:
        #     data.update({"NewsType": 3})
        # ret = self._batch_save(self.target_client, datas, "news_generate", ['Date', 'Title', 'Content', 'NewsType'])
        # print(ret)


if __name__ == "__main__":
    ch = ChangeTableSelector()
    ch.start()
