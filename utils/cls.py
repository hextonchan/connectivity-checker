"""
    Name:
        cls.py
    Desc:
        Simple script replicate java class.Forname
        https://stackoverflow.com/questions/452969/does-python-have-an-equivalent-to-java-class-forname
"""

class Class():
    def forName( kls ):
        parts = kls.split('.')
        module = ".".join(parts[:-1])
        m = __import__( module )
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m