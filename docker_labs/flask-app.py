from flask import Flask
from flask_limiter import Limiter 
from flask_limiter.util import get_remote_address

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["50 per day", "10 per minute"]
)

@app.route("/")
@limiter.limit("3 per minute")
def get_data():
    return {"data":"hello"}

if __name__=='__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
