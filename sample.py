import src.biu as biu  ## Must be the first line, because of monkey-included.


class MySpider(biu.Project):
    def start_requests(self):
        for i in range(0, 301, 30):
            # return 或者 yield 一个 biu.Request 就会去访问一个页面，参数与 requests 的那个基本上是兼容的
            yield biu.Request(
                url="https://www.douban.com/group/explore/tech?start={}".format(i),
                method="GET",
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
                },
                callback=self.parse,
            )

    def parse(self, resp):
        ## biu.Response 和 requests 的那个差不多，加了几个选择器上去
        for item in resp.xpath('//*[@id="content"]/div/div[1]/div[1]/div'):
            url = item.xpath("div[2]/h3/a/@href").extract_first()
            if url:
                yield biu.Request(
                    url=url,
                    method="GET",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
                    },
                    callback=self.parse_detail,
                )
            # return 或者 yield 一个 dict, 就会当作结果传到result_handler里进行处理

    def parse_detail(self, resp):
        yield {"url": resp.url}

    def result_handler(self, rv):
        print("get result:", rv)
        # 在这把你的结果存了


biu.run(MySpider(concurrent=1, interval=1, max_retry=5))
