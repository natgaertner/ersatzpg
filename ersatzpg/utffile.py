import chardet

class utffile(file):
    def next(self):
        n = super(utffile, self).next()
        if type(n) == unicode:
            return n.encode('utf-8')
        d = chardet.detect(n)
        return n.decode(d['encoding']).encode('utf-8')
