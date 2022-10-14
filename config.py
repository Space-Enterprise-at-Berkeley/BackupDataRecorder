from configparser import ConfigParser

def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    
    db = {}
    if parser.has_section(section):
        for p in parser.items(section):
            k = p[0]
            v = p[1]
            if len(v.split(", ")) == 1:
                db[k] = v
            else:
                db[k] = v.split(", ")
    else:
        raise Exception(f'Section{section} is not found in {filename}')
    
    return db
