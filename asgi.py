from main import app  # Import your Flask app

if __name__ == "__main__":
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    config = Config()
    config.bind = ["0.0.0.0:8000"]
    
    import asyncio
    asyncio.run(serve(app, config))
