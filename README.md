# Biu
A tiny web crawler framework

## Features
* 请使用 Python3.6 或更高版本
* 并发基于 Gevent，因此你必须在脚本一开始`import biu`，或者自行 monkey patch
* 请求基于 Requests，请求与请求结果的参数与 Requests 基本兼容
* 页面解析基于 Parsel, 因此使用方法与 Scrapy 一致
* 基本是一个缩水版的 Scrapy，用法与之非常类似
* 更多高级功能请面向源代码编程，自行发掘

## Installation
```
pip install biu
```

## Example
```python
import biu  ## Must be the first line, because of monkey-included.


class MySpider(biu.Project):
    def start_requests(self):
        for i in range(0, 301, 30):
            # return 或者 yield 一个 biu.Request 就会去访问一个页面，参数与 requests 的那个基本上是兼容的
            yield biu.Request(url="https://www.douban.com/group/explore/tech?start={}".format(i),
                              method="GET",
                              headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"},
                              callback=self.parse)

    def parse(self, resp):
        ## biu.Response 和 requests 的那个差不多，加了几个选择器上去
        for item in resp.xpath('//*[@id="content"]/div/div[1]/div[1]/div'):
            yield {
                "title": item.xpath("div[2]/h3/a/text()").extract_first(),
                "url": item.xpath("div[2]/h3/a/@href").extract_first(),
                "abstract": item.css("p::text").extract_first()
            }
            # return 或者 yield 一个 dict, 就会当作结果传到result_handler里进行处理


    def result_handler(self, rv):
        print("get result:", rv)
        # 在这把你的结果存了

biu.run(MySpider(concurrent=3, interval=0.2, max_retry=5))

```