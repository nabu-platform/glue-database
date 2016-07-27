package be.nabu.glue.database;

import java.util.ArrayList;
import java.util.List;

import be.nabu.glue.api.StaticMethodFactory;

public class DatabaseStaticMethodFactory implements StaticMethodFactory {

	@Override
	public List<Class<?>> getStaticMethodClasses() {
		List<Class<?>> classes = new ArrayList<Class<?>>();
		classes.add(DatabaseMethods.class);
		classes.add(DatabaseMethodsV2.class);
		return classes;
	}

}
