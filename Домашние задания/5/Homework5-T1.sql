--1.+
/*1. Для базы данных, которую вы создали после первого вебинара, напишите DDL код для создания таблиц. Проверяться будут соответствие имен,
  типизация, ограничения целостности. Все, что изображено на ER-диаграмме должно быть отражено в коде.
 */

/*
DROP TABLE T_Diet;
DROP TABLE T_Cage;
DROP TABLE T_Animals;
DROP TABLE S_Breeds;
DROP TABLE S_Room;
DROP TABLE S_Food;
*/

CREATE TABLE T_Animals (
	id_animals INTEGER NOT NULL,
	id_breed INTEGER NOT NULL,
	nickname VARCHAR2(30) NOT NULL,
	notes VARCHAR2(255),
	d_end DATE,
	n_end VARCHAR2(30),
	constraint PK_T_ANIMALS PRIMARY KEY (id_animals));

CREATE TABLE S_Breeds (
	id_breed INTEGER NOT NULL,
	name VARCHAR2(30) NOT NULL,
	d_end DATE,
	n_end VARCHAR2(30),
	constraint PK_S_BREEDS PRIMARY KEY (id_breed));

CREATE TABLE T_Diet (
	id_animals INTEGER NOT NULL,
	id_food INTEGER NOT NULL,
	feeding_time TIMESTAMP,
	d_end DATE,
	n_end VARCHAR2(30));

CREATE TABLE S_Food (
	id_food INTEGER NOT NULL,
	title VARCHAR2(30) NOT NULL,
	d_end DATE,
	n_end VARCHAR2(30),
	constraint PK_S_FOOD PRIMARY KEY (id_food));

CREATE TABLE T_Cage (
	id_cage INTEGER NOT NULL,
	id_animals INTEGER NOT NULL,
	id_room INTEGER NOT NULL,
	title VARCHAR2(30) NOT NULL,
	d_end DATE,
	n_end VARCHAR2(30),
	constraint PK_T_CAGE PRIMARY KEY (id_cage));

CREATE TABLE S_Room (
	id_room INTEGER NOT NULL,
	address VARCHAR2(30) NOT NULL,
	d_end DATE,
	n_end VARCHAR2(30),
	constraint PK_S_ROOM PRIMARY KEY (id_room));

ALTER TABLE T_Animals ADD CONSTRAINT FK_T_Animals_1 FOREIGN KEY (id_breed) REFERENCES S_Breeds(id_breed);

ALTER TABLE T_Diet ADD CONSTRAINT FK_T_Diet_1 FOREIGN KEY (id_animals) REFERENCES T_Animals(id_animals);
ALTER TABLE T_Diet ADD CONSTRAINT FK_T_Diet_2 FOREIGN KEY (id_food) REFERENCES S_Food(id_food);

ALTER TABLE T_Cage ADD CONSTRAINT FK_T_Cage_1 FOREIGN KEY (id_animals) REFERENCES T_Animals(id_animals);
ALTER TABLE T_Cage ADD CONSTRAINT FK_T_Cage_2 FOREIGN KEY (id_room) REFERENCES S_Room(id_room);
