import web

urls = (
        '/kommuner', 'kommuner'
        )

class kommuner:
    def GET(self):
        return "hello kommuner"

def main():
    app = web.application(urls, globals())
    app.run()

if __name__ == '__main__':
    main()
