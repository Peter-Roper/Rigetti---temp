from sqlalchemy import Column, String, Integer, ForeignKey, Float, CHAR, Boolean, DateTime, Text, or_
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from os import path
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import select, exists
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


################################ Classes ################################

class AnimalChannelList(Base):
    __tablename__ = 'AnimalChannelList'
    id = Column(Integer, primary_key=True)
    compound_animal = Column(String)
    channel = Column(String)


class DataFiles(Base):
    __tablename__ = 'DataFiles'
    animal = Column(String, primary_key=True)
    file_name = Column(String, primary_key=True)
                    # filename of the EEG file containing this animal/seizure
    file_path = Column(String)
                    # ditto file path
    file_start = Column(DateTime)
                    # date and time of beginning of file

    chan_number = Column(Integer)
    chan_idx = Column(Integer)
    sample_freq = Column(Integer)
    file_length = Column(Integer)
    video_file_path = Column(String)
    reviewed = Column(Boolean, unique=False, default=False)

class unusedDataFiles(Base):
    __tablename__ = 'unusedDataFiles'
    animal = Column(String, primary_key=True)
    file_name = Column(String, primary_key=True)
                    # filename of the EEG file containing this animal/seizure
    file_path = Column(String)
                    # ditto file path
    file_start = Column(DateTime)
                    # date and time of beginning of file

    chan_number = Column(Integer)
    chan_idx = Column(Integer)
    sample_freq = Column(Integer)
    file_length = Column(Integer)
    video_file_path = Column(String)

class AlgorithmParameters(Base):
    __tablename__ = 'AlgorithmParameters'

    animal = Column(String, primary_key=True)
        # parameters remain constant for each animal over duration of expt, so only need a single primary key
    window_size = Column(Integer)
        # how many ms is the window that we're looking at for one group? default = 120

    no_groups_per_epoch = Column(Integer)
        # how many groups to include in the epoch
        # epoch length is no_groups X window_size - default is 12s

    look_ahead = Column(Integer)
        # how far ahead (over how many groups) are we computing the autocorrelation?

    threshold = Column(Float) # when we've done the autocorrelation, and got the resulting metric
                              # get rid of everything that is within threshold X std-dev of the mean
                              # and what's left should just be the seizures

    proxthreshold = Column(Integer)
    durationthreshold = Column(Float)  # minimum length in seconds of a single 'event'
    ampthreshold = Column(Float)

    spikewindow = Column(Integer)   # when we're counting spikes, how big is the window over we deem a single
                              # spike can occur, measured in ms

    slope_scaling_factor = Column(Float)
    min_no_spikes = Column(Integer)

    min_spikerate = Column(Float)

    def __init__(self, animal=None):
        self.animal = animal
        self.no_groups_per_epoch = 100
        self.window_size = 500
        self.look_ahead = 2
        self.threshold = 1
        self.proxthreshold = 1
        self.durationthreshold = 2
        self.ampthreshold = 1
        self.spikewindow = 15
        self.min_spikerate = 0.35
        self.slope_scaling_factor = 5
        self.min_no_spikes = 15



class Events(Base):
    __tablename__ = 'Events'
    # using compound primary key composed of
    #       event_start
    #       animal
    #       filename
    animal = Column(String, primary_key=True)
                    # 'name' of the animal/channel, or channel label in acq
    filename = Column(String, primary_key=True)
                    # filename of the EEG file containing this animal/seizure
    filepath = Column(String)
                    # ditto file path
    event_start = Column(Float, primary_key=True)
                    # start time in number of seconds since the beginning of the file
    event_end = Column(Float)
                    # end time in number of seconds since the beginning of the file
    racine_score = Column(Integer)
    file_start = Column(DateTime)
                    # date and time of beginning of file
    meta_text = Column(Text)
                    # any general text associated with the file
    channel_no = Column(Integer)
                    # channel number in acq/smr file
    event_type = Column(String)
                    # what type of event is it?
    how_found = Column(String)
                    # either manually or automatically (man/auto)
    edit_status = Column(CHAR)
                    # edit status can be 'a' appended, 'j' joined, 'd' deleted, 'e' edited


################################ Functions ################################

def CreateDB(DBStr=None):
   # def execute(self):

    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)

    engine.dispose()

def EntryExists(DBStr=None, thisSession=None, thisDataFile=None, thisFileName=None, unusedDataFlag=True, AlgPars = None, thisEvent=None):

    if not thisSession:
        engine = create_engine(DBStr)

        session = sessionmaker()
        session.configure(bind=engine)
        Base.metadata.create_all(engine)
        s = session()
    else:
        s = thisSession

    if thisDataFile != None:
        longfilename = thisDataFile[0]
        thischandata = thisDataFile[1]
        thispath=path.dirname(path.abspath(longfilename))
        fname=path.basename(str(longfilename))

    if  thisFileName != None and unusedDataFlag == True:
        fname = path.basename(str(thisFileName))
        entry_query = s.query(unusedDataFiles).filter(unusedDataFiles.file_name==fname).all()
    elif  thisFileName != None and unusedDataFlag == False:
        fname = path.basename(str(thisFileName))
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==fname).all()
    elif thisDataFile != None and unusedDataFlag == False:
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==fname, DataFiles.animal==thischandata.name).all()
    elif  thisDataFile != None and unusedDataFlag == True:
        entry_query = s.query(unusedDataFiles).filter(unusedDataFiles.file_name==fname, unusedDataFiles.animal==thischandata.name).all()
    elif thisDataFile != None and unusedDataFlag == False:
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==fname, DataFiles.animal==thischandata.name).all()
    elif AlgPars != None:
        entry_query = s.query(AlgorithmParameters).filter(AlgorithmParameters.animal==AlgPars[0])

    elif thisEvent != None:
        entry_query = s.query(Events).filter(Events.event_start==thisEvent.Start,
                                                       Events.animal==thisEvent.Animal,
                                                       Events.filename==path.basename(thisEvent.FileName))
    if not thisSession:
        s.close()
        engine.dispose()

    if entry_query == None or entry_query==[]:
        return False
    else:
        return True

def MakeAnimalChanDict(DBStr=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    entry_query = s.query(AnimalChannelList).all()
    # first remove all the null entries
    for thisentry in reversed(entry_query):
        if thisentry.channel == None:
            entry_query.remove(thisentry)

    # now make dictionary
    AnimalChannelDict = {}
    for thisentry in entry_query:
        AnimalChannelDict[str(thisentry.channel)] = str(thisentry.compound_animal)
    s.close()
    engine.dispose()
    return AnimalChannelDict


def FindInDb(DBStr=None, thisTable=None, thisFilename = None, unusedDataFlag = True, thisAnimal=None, thisCompoundAnimal=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    if thisTable == 'Events':
        entry_query = s.query(Events).filter(Events.filename==path.basename(thisFilename), Events.animal==thisAnimal).all()
    if thisTable == 'unusedDataFiles':
        entry_query = s.query(unusedDataFiles).filter(unusedDataFiles.animal==thisAnimal).all()
    if thisTable == 'DataFiles' and thisAnimal==None:
        entry_query = s.query(DataFiles).filter(DataFiles.animal==thisAnimal).all()
    elif thisTable == 'DataFiles' and thisAnimal!=None:
        entry_query = s.query(DataFiles).filter(DataFiles.animal==thisAnimal, DataFiles.file_name==path.basename(thisFilename)).all()
    if thisTable == 'AnimalChannelList' and thisAnimal != None:
        entry_query = s.query(AnimalChannelList).filter(AnimalChannelList.channel==thisAnimal).one()
    if thisTable == 'AnimalChannelList' and thisCompoundAnimal != None:
        entry_query = s.query(AnimalChannelList).filter(AnimalChannelList.compound_animal==thisCompoundAnimal).all()
    s.close()
    engine.dispose()
    return entry_query
    s.commit()

def MoveAnimals(DBStr=None, oldTable=None, newTable=None, thisFilename = None, unusedDataFlag = True, thisAnimal=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    if oldTable == 'unusedDataFiles':
        oldTable = unusedDataFiles
        newTable = DataFiles
    elif oldTable== 'DataFiles':
        oldTable = DataFiles
        newTable = unusedDataFiles

    entry_query = s.query(oldTable).filter(oldTable.animal==thisAnimal).all()

    for olddatafile in entry_query:
               # if unusedDataFlag:
        thisdatafile = newTable(file_name=olddatafile.file_name)
       # else:
        #    thisdatafile = DataFiles(file_name=fname)
        thisdatafile.animal = olddatafile.animal
        thisdatafile.file_path = olddatafile.file_path
        thisdatafile.chan_idx=olddatafile.chan_idx
        thisdatafile.file_length=olddatafile.file_length
        thisdatafile.chan_number=olddatafile.chan_number
        thisdatafile.sample_freq=olddatafile.sample_freq
        try:
            # if we don't have a valid file start then skip this step
            thisdatafile.file_start = olddatafile.file_start
        except:
            pass
        s.add(thisdatafile)

    # now delete from old table
    entry_query = s.query(oldTable).filter(oldTable.animal==thisAnimal)

    if entry_query == None or entry_query==[]:
        s.close()
        engine.dispose()
        return False
    elif type(entry_query) is list:
        for this_query in entry_query:
            this_query.delete()
    else:
        entry_query.delete()
    s.commit()
    s.close()
    engine.dispose()
    return

def UpdateDb(DBStr=None, thisDataFile = None, unusedDataFlag = True, thisAnimal=None, thisVideoPath=None, fileReviewed=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()
    if thisVideoPath != None:
        s.query(DataFiles).filter(DataFiles.animal==thisAnimal).update({'video_file_path': thisVideoPath})
    if fileReviewed != None:
        s.query(DataFiles).filter(DataFiles.animal==thisAnimal, DataFiles.file_name==thisDataFile).update({'reviewed': fileReviewed})
    s.commit()
    s.close()
    engine.dispose()
    return
   # stmt = DataFiles.update().where(DataFiles.Animal==thisAnimal).values(name='user #5')

def AddToDb(DBStr=None, thisSession=None, thisDataFile = None, unusedDataFlag = True, AlgPars=None,
            thisEvent=None, thisCompoundAnimal=None, thisChannel=None):
    if not thisSession:
        engine = create_engine(DBStr)

        session = sessionmaker()
        session.configure(bind=engine)
        Base.metadata.create_all(engine)
        s = session()
    else:
        s = thisSession

    if thisDataFile != None:
        longfilename = thisDataFile[0]
        chandata = thisDataFile[1]
        thispath=path.dirname(path.abspath(longfilename))
        fname=path.basename(str(longfilename))

        if unusedDataFlag:
            thisdatafile = unusedDataFiles(file_name=fname)
        else:
            thisdatafile = DataFiles(file_name=fname)
        thisdatafile.animal = chandata.name
        thisdatafile.file_path = thispath
        thisdatafile.chan_idx=chandata.idx
        thisdatafile.file_length=chandata.file_length
        thisdatafile.chan_number=chandata.number
        thisdatafile.sample_freq=chandata.sample_freq
        try:
            # if we don't have a valid file start then skip this step
            thisdatafile.file_start = datetime.datetime(*chandata.file_start[:6])
        except:
            pass

    if thisEvent != None:
        thisdatafile=Events(animal=thisEvent.Animal,
                                      filename=path.basename(thisEvent.FileName),
                                      event_start=thisEvent.Start)
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==path.basename(thisEvent.FileName)).first()
        thisdatafile.file_start = entry_query.file_start

        thisdatafile.event_end=thisEvent.End
        thisdatafile.meta_text=thisEvent.Description
        thisdatafile.racine_score=thisEvent.RacineScore
        thisdatafile.filepath = path.dirname(path.abspath(thisEvent.FileName))
        thisdatafile.event_type=thisEvent.Event
        thisdatafile.how_found = thisEvent.HowFound

    if thisCompoundAnimal!=None:
        if thisChannel == None:
            thisdatafile = AnimalChannelList(compound_animal=thisCompoundAnimal)
        else:
            thisdatafile = AnimalChannelList(compound_animal=thisCompoundAnimal, channel=thisChannel)


    if AlgPars != None:# and not unusedDataFlag:
        thisdatafile=AlgorithmParameters(animal = AlgPars[0])

    s.add(thisdatafile)
    if not thisSession:
        s.commit()
    if not thisSession:
        s.close()
        engine.dispose()
    return

def RemoveFromDb(DBStr=None, thisTable=None, thisFileName=None, thisAnimalName=None, thisDataFile=None, unusedDataFlag=True, AlgPars = None, thisEvent=None):

    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    if thisDataFile != None:
        longfilename = thisDataFile[0]
        thischandata = thisDataFile[1]
        thispath=path.dirname(path.abspath(longfilename))
        fname=path.basename(str(longfilename))

    if thisFileName != None and thisTable != None:
        if thisTable == 'DataFiles':
            thisTable = DataFiles
        elif thisTable == 'unusedDataFiles':
            thisTable = unusedDataFiles

        entry_query = s.query(thisTable).filter(or_(thisTable.file_name==thisFileName))

    elif thisTable == 'AnimalChannelList' and thisAnimalName != None:
        entry_query = s.query(AnimalChannelList).filter(AnimalChannelList.compound_animal==thisAnimalName)
    elif  thisDataFile != None and unusedDataFlag == True:
        entry_query = s.query(unusedDataFiles).filter(unusedDataFiles.file_name==fname, unusedDataFiles.animal==thischandata.name).all()
    elif thisDataFile != None and unusedDataFlag == False:
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==fname, DataFiles.animal==thischandata.name).all()
    elif thisFileName != None:
        entry_query = s.query(DataFiles).filter(DataFiles.file_name==thisFileName)
    elif AlgPars != None:
        entry_query = s.query(AlgorithmParameters).filter(AlgorithmParameters.animal==AlgPars[0])
    elif thisAnimalName != None:
        entry_query = s.query(DataFiles).filter(DataFiles.animal==thisAnimalName)

    elif thisEvent != None:
        entry_query = s.query(Events).filter(Events.event_start==thisEvent.Start,
                                                       Events.animal==thisEvent.Animal,
                                                       Events.filename==path.basename(thisEvent.FileName))

    if entry_query == None or entry_query==[]:
        return False
    # elif type(entry_query) is list:
    #     for this_query in entry_query:
    #         this_query.delete()
    else:
        entry_query.delete()
    s.commit()
    s.close()
    engine.dispose()

    return entry_query


def GetDistinctValues(DBStr=None, TableName = None, ColName=None):
    import time
#    start = time.time()
#    print 'gsv start = ', start

    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()
    if TableName == 'DataFiles':
        TableName = DataFiles
    elif TableName == 'unusedDataFiles':
        TableName = unusedDataFiles
    elif TableName == 'AnimalChannelList':
        TableName = AnimalChannelList
    if ColName == 'file_name':
        #stop = time.time() - start
        #print 'gsv step1 = ', stop
        distinct_files = []
        for file in s.query(TableName.file_name).distinct():
            print file
            distinct_files.append(str(file[0]))
        #stop = time.time() - start
        #print 'gsv step2 = ', stop
        s.close()
        engine.dispose()
        return distinct_files
    if ColName == 'animal':
        distinct_animals = []
        for file in s.query(TableName.animal).distinct():
            print file
            distinct_animals.append(str(file[0]))
        s.close()
        engine.dispose()
        return distinct_animals
    if ColName == 'compound_animal':
        distinct_animals = []
        for file in s.query(TableName.compound_animal).distinct():
            print file
            distinct_animals.append(str(file[0]))
        s.close()
        engine.dispose()
        return distinct_animals
    if ColName == 'channel':
        distinct_animals = []
        for file in s.query(TableName.channel).distinct():
            print file
            distinct_animals.append(str(file[0]))
        s.close()
        engine.dispose()
        return distinct_animals
    else:
        print 'no field selected, returning zilch'
        s.close()
        engine.dispose()
        return []


def GetAllChans(DBStr=None, this_filename=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    all_chans = s.query(DataFiles).filter(DataFiles.file_name == this_filename).all()
    s.close()
    engine.dispose()
    return all_chans

def GetAllFiles(DBStr=None, this_animal_name=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    all_files = s.query(DataFiles).filter(DataFiles.animal == this_animal_name).all()
    s.close()
    engine.dispose()
    return all_files


def GetAllSeizures(DBStr=None, this_animal_name=None):
    engine = create_engine(DBStr)

    session = sessionmaker()
    session.configure(bind=engine)
    Base.metadata.create_all(engine)
    s = session()

    all_szrs = s.query(Events).filter(Events.animal == this_animal_name).all()
    s.close()
    engine.dispose()
    return all_szrs
