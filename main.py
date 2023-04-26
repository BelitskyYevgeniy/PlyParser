import os
from collections.abc import Iterable
import numpy as np

from flask import Flask, jsonify, abort, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

from plyfile import PlyData
from PlyParser import PlyParser

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ply.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app, session_options={"autoflush": False})

PropertyValueSize = 250

class File(db.Model):
    __tablename__ = 'File'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(250), nullable=False, unique=True)
    header = db.Column(db.String(500), nullable=False)

    elements = db.relationship("Element", back_populates="file", lazy=True)

    def __bytes__(self):
        result = bytes(self.header + '\n', 'utf-8')
        for element in self.elements:
            result += bytes(element) + b'\n'
        return result

class ElementType(db.Model):
    __tablename__ = 'ElementType'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(250), nullable=False, unique=True)

    elements = db.relationship("Element", back_populates='type')

class Element(db.Model):
    __tablename__ = 'Element'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    count = db.Column(db.Integer, nullable=False)
    file_id = db.mapped_column(db.ForeignKey('File.id'), nullable=False)
    type_id = db.mapped_column(db.ForeignKey('ElementType.id'), nullable=False)
    property_head_id = db.mapped_column(db.ForeignKey('Property.id'))

    file = db.relationship("File", back_populates='elements')
    type = db.relationship("ElementType", back_populates='elements', lazy=True)
    property_head = db.relationship("Property", back_populates='element', lazy=True)

    def __bytes__(self):
        return bytes(self.property_head)

class Property(db.Model):
    __tablename__ = 'Property'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    next_property_id = db.mapped_column(db.ForeignKey('Property.id'), nullable=True)
    value = db.Column(db.BLOB(PropertyValueSize), nullable=False)

    element = db.relationship("Element", back_populates='property_head')
    next_property = db.relationship("Property", remote_side=[id], lazy=True)

    def __bytes__(self):
        result = self.value
        if self.next_property is not None:
            result += bytes(self.next_property)
        return result

def add_property(property):
    chunks = [ property[i:i+PropertyValueSize] for i in range(0, len(property), PropertyValueSize)]

    start = len(chunks) - 1
    next = None

    entity = Property()
    entity.value = chunks[start]
    entity.next_property = next
    db.session.add(entity)

    start -= 1
    next = entity

    while start >= 0:
        entity = Property()
        entity.value = chunks[start]
        entity.next_property = next
        db.session.add(entity)

        start -= 1
        next = entity
    return entity

def ensure_element_type_exist(element_type):
    entity = ElementType.query.filter(ElementType.name == element_type).first()
    if entity is None:
        entity = ElementType()
        entity.name = element_type
        db.session.add(entity)
    return entity

def add_element(element):
    entity = Element()
    entity.count = element.count
    entity.type = ensure_element_type_exist(element.name)
    entity.property_head = add_property(element.properties)

    db.session.add(entity)
    return entity

def add_file(filename, ply_data):
    file = File()
    file.name = filename
    file.header = ply_data.headers
    for element in ply_data.elements:
        file.elements.append(add_element(element))
    db.session.add(file)
    db.session.commit()

@app.route('/files/<filename>', methods=['POST'])
def send_file(filename):
    entity = File.query.filter(File.name == filename).first()
    if entity is not None:
        abort(400, 'File is already exist')
    ply_content = request.data
    # ply_content = ply_content.replace('\r', '')
    try:
        # with open(filename, "ab") as f:
        #    f.write(ply_content)
        # ply_data = PlyData.read(filename)
        # ply_data.text = True
        # ply_data.write('file.txt')
        # os.remove(filename)
        parser = PlyParser()
        ply_file = parser.parse(ply_content)

    except Exception as e:
        abort(400, 'Wrong ply file format')

    try:
        add_file(filename, ply_file)
    except Exception as e:
        abort(400, str(e))
    return "", 201

@app.route('/files', methods=['GET'])
def get_filenames():
    filenames = [e[0] for e in File.query.with_entities(File.name).all()]
    return jsonify({'filenames': filenames})


@app.route('/files/content', methods=['GET'])
def get_by_name():
    filename = request.args.get('filename')
    entity = File.query.filter(File.name == filename).first()
    if entity is None:
        abort(404, 'File not found')
    return bytes(entity)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
