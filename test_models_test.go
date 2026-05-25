package mx

type User struct {
	DefaultTime    `json:"default_time"`
	ID             uint32 `json:"id"`
	Name           string `json:"name"`
	Age            int    `json:"age"`
	UID            int    `json:"uid"`
	IgnoreMe       int    `mx:"-" json:"ignore_me"`
	AfterFindCount int    `mx:"-" json:"after_find_count"`
	Weapon         Weapon `json:"weapon"`
	Gems           []Gem  `json:"gem"`
}

type Weapon struct {
	ID     int    `json:"id"`
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
	Lv     string `json:"lv"`
	DefaultTime
}

type Gem struct {
	ID             int       `json:"id"`
	UserID         int       `json:"user_id"`
	Name           string    `json:"name"`
	Lv             string    `json:"lv"`
	AfterFindCount int       `mx:"-" json:"after_find_count"`
	History        []History `json:"history"`
	DefaultTime
}

type History struct {
	ID     int    `json:"id"`
	Remark string `json:"remark"`
}

func (g *Gem) AfterFind() error {
	g.AfterFindCount++
	return nil
}

func (u *User) AfterFind() error {
	u.AfterFindCount++
	return nil
}

type DefaultTime struct {
	Ctime string `json:"ctime"`
	Utime string `json:"utime"`
}
