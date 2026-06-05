# QuartzDB query language (QtzQL)


```text
_time:5m -> timestamp filter
_stream  -> filter
_stream -> _stream(f1=v1, f2=v2, f3=v3) -> will create a stream id
_source.foo.bar:"fetch" -> row search
_labels.foo.bar~="gefd*" ->tantivy search, get transform into tantivy query on _qtz_labels.foo.bar
foo=12 or bar~=ff && time(start end) or  
_tag.host=12 -> tag filter


```
