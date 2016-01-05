# importing libraries
import sqlalchemy
from sqlalchemy import Column, Integer, String, Sequence, text, func, create_engine, and_, or_
from sqlalchemy import ForeignKey, Table, Text
# allow creation of classes that include directives describing db table
from sqlalchemy.ext.declarative import declarative_base
# object relational ones
from sqlalchemy.orm import sessionmaker, aliased, relationship, joinedload, contains_eager
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
# sql ones
from sqlalchemy.sql import exists

# declarative_base creates a base class
# to define any number of mapped classes
Base = declarative_base()

# echo flag True for logging
# create_engine returns engine instance,
# and creates connection to db
Engine = create_engine('sqlite:///:memory:', echo=True)

# create a session and connect current engine to it
# later, to instantate a session >> session = session()
Session = sessionmaker(bind=Engine)
session = Session()

# creating sessions: session = Session()
# define classes with session.query()
# try: things.go(session), session.commit()
# except: session.rollback(); raise
# finally: session.close()
###
# try to keep sessions, transaction, exception management 
# separate from program details

# create class User, table users
# class using declarative needs at least tablename 
# and column with primary key
class User(Base):
	__tablename__ = 'users'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	password = Column(String)

	def __repr__(self):
		return "<User(name='%s', password='%s'>" % ( 
			self.name, self.password)


Base.metadata.create_all(Engine)

# create a mapped class instance
ed_user=User(name='ed', password='edespassword')
session.add(ed_user)
session.add_all([User(name='wendy', password='foo'), User(name='bed', password='bar')])

# change ed's pw
ed_user.password='complex'

# what's been modified?
session.dirty

# what's been added?
session.new

# commit changes
# this also sets the user id which was previously None for ed and everyone else.
session.commit()

# change stuff and then rollback(revert)
ed_user.name='edwardo'
fake_user=User(name='fakeuser', password='123')
session.add(fake_user)
#	.filter(User.name.in_(['edwardo','fakeuser'])).all()
session.rollback()
#	.filter(User.name.in_(['ed','fakeuser'])).all()
fake_user in session

# query stuff
for instance in session.query(User).order_by(User.id):
	print(instance.name)
for row in session.query(User, User.name).all():
	print(row.User, row.name)

# querying with labels
for row in session.query(User.name.label('name_label')).all():
	print(row.name_label)

# using aliases: user_alias = user
user_alias = aliased(User, name='user_alias')
for row in session.query(user_alias, user_alias.name).all():
	print(row.user_alias)

# making fancier queries
for u in session.query(User).order_by(User.id)[1:3]:
	print(u)
for name, in session.query(User.name).filter_by(name='ed'):
	print(name)
# each query call returns a new query object (can add more)
for user in session.query(User).filter(User.name=='ed').filter(User.name=='wendy'):
	print(user)

# filter, not, null/none, and, or, match
# #	.filter(User.name[=, !=]'yourname')
# #	.filter(User.name
#	.like(%name%))
#	.in(['wendy', 'bob'])
# #	.filter(~User.name.in_(['wendy', 'bob']))
# == None || .is_(None), != None || .isnot(None)
#	.filter(and_(User.name=='ed', User.name=='wendy'))
#	.filter(User.name=='ed', User.name=='wendy')
#	.filter(User.name=='ed'.filter(User.name=='wendy'))
#	.filter(or_(User.name=='ed', User.name=='wendy'))

# match is database specific; depends on backend
#	.filter(User.name.match('wendy'))

# list and scalar return
query=session.query(User).filter(User.name.like('%ed%')).order_by(User.id)
print(query.all())
print(query.first())

# one() returns all rows and error if there are multiple rows found
# good to handle multiple and no results differently
try:
	usernotone=query.one()
except:
	print("MutlipleResultsFound")
try:
	usernone=query.filter(User.id==99).one()
except:
	print("NoResultFound")
# one_or_none() only error when multiple
# scalar() invokes one(), returns first column of row if success

# using literal strings
for user in session.query(User).filter(text("id<200")).order_by(text("id")).all():
	print(user.name)
# binding parameters
session.query(User).filter(text("id<:value and name=:name")).params(value=200, name='ed').order_by(User.id).one()
# why use SQL in sqlalchemy though
# why # 1
session.query(User).from_statement(text("SELECT * FROM users where name=:name")).params(name='ed').all()
# why # 2
stmt = text("SELECT name, id FROM users where name=:name")
stmt = stmt.columns(User.name, User.id)
session.query(User).from_statement(stmt).params(name='ed').all()

# count() number of returned rows
num_eds=session.query(User).filter(User.name.like('%ed%')).count()
print(num_eds)

# func.count() specify which things to count 
counts=session.query(func.count(User.name), User.name).group_by(User.name).all()
print(counts)

# or can use it like reg count()
regcount=session.query(func.count('*')).select_from(User).scalar()
regcount=session.query(func.count(User.id)).select_from(User).scalar()
print(regcount)

# foreign keys and relationships
# add a new class and map it to User class
class Address(Base):
	__tablename__='addresses'
	id=Column(Integer, primary_key=True)
	email=Column(String, nullable=False)
	user_id=Column(Integer, ForeignKey('users.id'))

	user = relationship("User", back_populates='addresses')

	def __repr__(self):
		return "<Address(email='%s')>" % self.email

# back_populates is like backref
# both sides will listen in on each other
# so only ned to do a back_populates once to establish the relationship
User.addresses = relationship("Address", order_by=Address.id, back_populates="user")

# we gotta add the metadata for address class and its relationship with User
Base.metadata.create_all(Engine)

# adding users with addresses
jack = User(name='jack', password='jack')
print(jack.addresses) # this is nothing
jack.addresses=[Address(email='jack@nitz.org'), Address(email='spamone@mail.co')]
# accessing array of addresses
print(jack.addresses)
# can access user class too, because of their relationship
print(jack.addresses[1].user)
# add jack and commit
session.add(jack)
session.commit()
print(jack.addresses)

# implicit join
for u, a in session.query(User, Address).filter(User.id==Address.user_id).filter(Address.email=='jack@nitz.org').all():
	print(u)
	print(a)
# explicit join
session.query(User).join(Address).filter(Address.email=='jack@nitz.org').all()

# how to joins stuff
# query.join(
#	Address, User.id==Address.user.id)		# explicit
#	User.addresses)							# left to right relationship
#	Address, User.addresses)				# like above with target
#	'addresses')							# like above with string	
# .outerjoin()

# use select_from(class) when:
q = session.query(User, Address).select_from(Address).join(User)

alias1=aliased(Address)
alias2=aliased(Address)
for username,email1, email2 in session.query(User.name, alias1.email, alias2.email).join(alias1, User.addresses).join(alias2, User.addresses).filter(alias1.email=='jack@nitz.org').filter(alias2.email=='spamone@mail.co'):
	print(username,email1,email2)

# exists: exactly what it seems it is - returns boolean
# using it explicitly
stmt = exists().where(Address.user_id==User.id)
for name, in session.query(User.name).filter(stmt):
	print(name)

# using any() which uses exists()
for name, in session.query(User.name).filter(User.addresses.any()):
	print(name)

# comparisons 
# == many to one equals
# != many to one not equals
# == None many to one is null
# .contains() one to many collections
# .any() collections
# .has() scalars
# .with_parent() any relationships

# options() for joins and loads 
# joinedload()
# query will emit extra join
jack = session.query(User).options(joinedload(User.addresses)).filter_by(name='jack').one()
print(jack)
# eager load
jacks_addresses=session.query(Address).join(Address.user).filter(User.name=='jack').options(contains_eager(Address.user)).all()
print(jacks_addresses)

# deleting
session.delete(jack)
print(session.query(User).filter_by(name='jack').count()) # print 0
# but his addresses still there (not deleted yet)
print(session.query(Address).filter(Address.email.in_(['jack@nitz.org','spamone@mail.co'])).count())

# need to add cascade behaviour - close session and start over again
session.close()
Base=declarative_base()
class User(Base):
	__tablename__ = 'users'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	password = Column(String)

	addresses = relationship("Address", back_populates='user',cascade="all, delete, delete-orphan")

	def __repr__(self):
		return "<User(name='%s', password='%s'>" % (self.name, self.password) 

class Address(Base):
	__tablename__='addresses'
	id=Column(Integer, primary_key=True)
	email=Column(String, nullable=False)
	user_id=Column(Integer, ForeignKey('users.id'))
	user = relationship("User", back_populates='addresses')

	def __repr__(self):
		return "<Address(email='%s')>" % self.email	

Base.metadata.create_all(Engine)

jack=session.query(User).get(4)
print(jack.addresses)
del jack.addresses[1]
print(jack.addresses)
session.delete(jack)
print(session.query(User).filter_by(name='jack').count())

# Mappings
# post and keywords both refer to post_keywords
# association table
post_keywords=Table('post_keywords', Base.metadata,
	Column('post_id', ForeignKey(posts.id), primary_key=True),
	Column('keyword_id', ForeignKey(keywords.id), primary_key=True))

class Post(Base):
	__tablename__='posts'
	id = Column(Integer, primary_key=True)
	user_id=Column(Integer, ForeignKey('users.id'))
	headline=Column(String(200), nullable=False)

	#many to many post to keywords
	keywords=relationship('Keyword', secondary=post_keywords, back_populates='posts')

	# the init method is optional when using declarative though
	def __init__(self, headline): 
		self.headline = headline

	def __repr__(self):
		return "Post(%r, %r)" % (self.headline)

class Keyword(Base):
	__tablename__='keywords'
	id = Column(Integer, primary_key=True)
	keyword=Column(Integer, nullable=False, unique=True)
	posts=relationship('Post', secondary=post_keywords, back_populates='keywords')

	def __init__(self, keyword): 
		self.keyword = keyword

Base.metadata.create_all(engine)

