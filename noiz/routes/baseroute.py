from flask import current_app

@current_app.route('/uhm')
def uhm():
    return 'Twoja stara kreci lody'
