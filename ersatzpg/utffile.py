import chardet

class utffile(file):
    def next(self):
        n = super(utffile, self).next()
        d = chardet.detect(n)
        return n.decode(d['encoding']).encode('utf-8')
