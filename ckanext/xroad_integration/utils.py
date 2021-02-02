def init_db():
    import ckan.model as model
    from ckanext.xroad_integration.model import init_table
    init_table(model.meta.engine)