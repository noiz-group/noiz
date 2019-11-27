from noiz.database import db
from noiz.models import ProcessingParams


def upsert_default_params():
    default_config = ProcessingParams(id=1)
    current_config = ProcessingParams.query.filter_by(id=1).first()

    if current_config is not None:
        db.session.merge(default_config)
    else:
        db.session.add(default_config)
    db.session.commit()

    return
