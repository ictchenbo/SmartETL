from smartetl.gestata.world_country import ENGLISH_TO_CHINESE_COUNTRIES


def as_import_export(row: dict):
    # _this = row.get('reporterDesc')
    source = row['filename']
    _this = source[:source.rfind('.')]

    _that = row.get('partnerDesc')
    _that = ENGLISH_TO_CHINESE_COUNTRIES.get(_that) or _that

    name = row.get('cmdDesc')
    trade_type = row.get('customsDesc')
    trade_amount = row.get('primaryValue')
    date = str(row.get('refPeriodId'))
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]

    ret = dict(name=name, date=date, trade_type=trade_type, trade_amount=trade_amount)
 
    direction = row.get('flowDesc').lower() 
    if 'import' in direction:
        ret['import_location'] = _this
        ret['export_location'] = _that
    else:
        ret['import_location'] = _that
        ret['export_location'] = _this
    
    return ret
