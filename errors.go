package roada

import "errors"

var ErrKeyDuplicate = errors.New("key duplicate")

var ErrNodeDuplicate = errors.New("node duplicate")

var ErrKeyNotFound = errors.New("key not found")

var ErrNodeNotFound = errors.New("node not found")

var ErrRouteIllFormed = errors.New("service/method ill-formed")

var ErrNodeConflict = errors.New("node conflict")
var ErrKeyConflict = errors.New("key conflict")

var ErrArgsCount = errors.New("args count")

var ErrSlaveMergeFormat = errors.New("slave merge format")

var ErrRegistryRestoreFormat = errors.New("registry restore format")

var ErrRestoreDataDirNotFound = errors.New("registry restore data dir not found")

var ErrKeyTypeNotGroup = errors.New("key type not group")

var ErrMemberNotFound = errors.New("member not found")

var ErrMemberNotOwn = errors.New("member not own")

var ErrKeyTypeNotUnique = errors.New("key type not unique")

var ErrGroupMemberDuplicate = errors.New("group member duplicate")

var ErrMergeVersionErr = errors.New("merge version err")
