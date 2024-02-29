"""
This module manages the LAPW species files in the local repository.
"""

import os
from aiida.orm.querybuilder import QueryBuilder
from aiida.orm import SinglefileData
from aiida.common.lang import classproperty
from aiida.common.files import md5_file


LAPWBASIS_GROUP_TYPE = "data.lapwbasis.family"


def parse_species_file(filename):
    """
    Parse .in files
    """
    properties = {}

    with open(filename) as data_file:
        properties["chemical_symbol"] = data_file.read()[:10].replace("'", "").strip()

    return properties


def upload_family(folder, group_name, group_description, stop_if_existing=True):
    """
    Upload a set of LAPW species files in a given group.

    :param folder: a path containing all LAPW species files to be added.
        Only files ending in .xml or .json (case-insensitive) are considered.
    :param group_name: the name of the group to create. If it exists and is
        non-empty, a UniquenessError is raised.
    :param group_description: a string to be set as the group description.
        Overwrites previous descriptions, if the group was existing.
    :param stop_if_existing: if True, check for the md5 of the files and,
        if the file already exists in the DB, raises a MultipleObjectsError.
        If False, simply adds the existing LapwbasisData node to the group.
    """

    import aiida.common

    # from aiida.common import aiidalogger
    from aiida.orm import Group
    from aiida.common.exceptions import UniquenessError, NotExistent
    from aiida.orm import User

    if not os.path.isdir(folder):
        raise ValueError("folder must be a directory")

    # only files, and only those ending with .xml or .XML or .json or .JSON
    # go to the real file if it is a symlink
    files = [
        os.path.realpath(os.path.join(folder, i))
        for i in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, i)) and i.lower().endswith(".in")
    ]

    nfiles = len(files)
    user = User.collection.get_default()

    # NB: this does not work
    # > g = Group(label="oi", type_string="naaa")
    # > g.type_string
    # 'core'
    try:
        group = Group.collection.get(label=group_name, type_string=LAPWBASIS_GROUP_TYPE)
        group_created = False
    except NotExistent:
        group = Group(label=group_name, type_string=LAPWBASIS_GROUP_TYPE, user=user)
        group_created = True

    if group.user.id != user.id:
        raise UniquenessError(
            "There is already a Lapwbasis group with name {}"
            ", but it belongs to user {}, therefore you "
            "cannot modify it".format(group_name, group.user.email)
        )

    # Always update description, even if the group already existed
    group.description = group_description

    # find species that are already in the group
    md5_list = []
    if not group_created:
        for aiida_n in group.nodes:
            # Skip other nodes
            if isinstance(aiida_n, LapwbasisData):
                md5_list.append(aiida_n.md5)

    # NOTE: GROUP IS SAVED ONLY AFTER CHECKS OF UNICITY

    species_list = []
    for f in files:
        sp = LapwbasisData.get_or_create(f, use_first=not stop_if_existing)
        if not sp[0].md5 in md5_list:
            md5_list.append(sp[0].md5)
            species_list.append(sp)

    # At this point, save the group, if still unstored
    if group_created:
        group.store()

    # save species in the database, and add them to group
    for sp, created in species_list:
        if created:
            sp.store()

    # Add elements to the group all togetehr
    group.add_nodes([sp for sp, created in species_list])

    nuploaded = len([sp for sp, created in species_list if created])

    return nfiles, nuploaded


class LapwbasisData(SinglefileData):

    def __init__(self, **kwargs):
        """Construct a new instance and set the contents to that of the file.

        :param file: an absolute filepath of the file to wrap
        """
        super().__init__(**kwargs)

    @classproperty
    def lapwbasisfamily_type_string(cls):
        return LAPWBASIS_GROUP_TYPE

    @property
    def chemical_symbol(self):
        return self.base.attributes.get("chemical_symbol", None)

    @property
    def md5(self):
        return self.base.attributes.get("md5", None)

    @classmethod
    def get_or_create(cls, filename, use_first=False):
        """
        Pass the same parameter of the init; if a file with the same md5
        is found, that LapwbasisData is returned. 
        
        :param filename: an absolute filename on disk
        :param use_first: if False (default), raise an exception if more than \
                one species is found.\
                If it is True, instead, use the first available LAPW species.
        :return (lapwbasis, created): where lapwbasis is the LapwbasisData object, and create is either\
            True if the object was created, or False if the object was retrieved\
            from the DB.
        """
        import aiida.common.utils
        import os
        from aiida.common.exceptions import ParsingError

        if not os.path.abspath(filename):
            raise ValueError("filename must be an absolute path")

        md5sum = md5_file(filename)
        species = cls.from_md5(md5sum)

        if len(species) == 0:
            properties = parse_species_file(filename)
            instance = cls(file=filename)
            instance.base.attributes.set("md5", md5sum)
            instance.base.attributes.set(
                "chemical_symbol", properties["chemical_symbol"]
            )
            return (instance, True)
        elif len(species) == 1 or use_first:
            return (species[0], False)
        else:
            raise ValueError(
                "More than one copy of a species file "
                "with the same MD5 has been found in the "
                "DB. pks={}".format(",".join([str(i.pk) for i in species]))
            )

    @classmethod
    def from_md5(cls, md5, backend=None):
        """
        Return a list of all LAPW species that match a given MD5 hash.

        Note that the hash has to be stored in a _md5 attribute, otherwise
        the species will not be found.
        """

        builder = QueryBuilder(backend=backend)
        builder.append(cls, filters={"attributes.md5": {"==": md5}})
        return builder.all(flat=True)

    @classmethod
    def get_lapwbasis_group(cls, group_name):
        from aiida.orm import Group

        return Group.get(name=group_name, type_string=LAPWBASIS_GROUP_TYPE)

    @classmethod
    def get_lapwbasis_groups(cls, filter_elements=None, user=None, backend=None):
        """
        Return all names of groups of type LapwbasisData, possibly with some filters.

        :param filter_elements: A string or a list of strings.
               If present, returns only the groups that contains one Lapwbasis for
               every element present in the list. Default=None, meaning that
               all families are returned.
        :param user: if None (default), return the groups for all users.
               If defined, it should be either a DbUser instance, or a string
               for the username (that is, the user email).
        """
        from aiida.orm import Group

        group_query_params = {"type_string": cls.lapwbasisfamily_type_string}

        if user is not None:
            group_query_params["user"] = user

        if isinstance(filter_elements, str):
            filter_elements = [filter_elements]

        if filter_elements is not None:
            actual_filter_elements = {_.capitalize() for _ in filter_elements}

            group_query_params["node_attributes"] = {
                "chemical_symbol": actual_filter_elements
            }

        # all_lapwbasis_groups = Group.query(**group_query_params)
        builder = QueryBuilder(backend=backend)
        builder.append(Group, filters=group_query_params)
        all_lapwbasis_groups = builder.all(flat=True)

        groups = [(g.name, g) for g in all_lapwbasis_groups]
        # Sort by name
        groups.sort()
        # Return the groups, without name
        return [_[1] for _ in groups]
